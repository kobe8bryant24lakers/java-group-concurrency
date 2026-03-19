package io.github.kobe.internal;

import io.github.kobe.GroupPolicy;
import io.github.kobe.GroupResult;
import io.github.kobe.GroupStats;
import io.github.kobe.TaskLifecycleListener;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;


/**
 * Manages per-group state (semaphore + explicit queue) and dispatching logic.
 *
 * <p>Each group has a {@link GroupState} containing:
 * <ul>
 *   <li>A fair {@link Semaphore} capping concurrent virtual threads (= maxConcurrency)</li>
 *   <li>A {@link LinkedBlockingQueue} holding tasks that are waiting for a semaphore permit</li>
 * </ul>
 *
 * <p>The dispatch model:
 * <ol>
 *   <li>On submit: {@code tryAcquire()} the semaphore — if successful, dispatch immediately;
 *       otherwise {@code offer()} to the queue.</li>
 *   <li>On task done: {@code poll()} the queue — if a task is waiting, hand off the permit
 *       directly (no release/acquire round-trip); otherwise release the permit and call
 *       {@code tryDispatch()} to close the race window.</li>
 *   <li>{@code tryDispatch()} handles the race where a task was enqueued between
 *       {@code queue.poll()} returning null and {@code semaphore.release()}.</li>
 * </ol>
 */
public final class GroupStateManager {

    /**
     * Immutable state for a single group.
     */
    public static final class GroupState {
        public final Semaphore semaphore;
        public final LinkedBlockingQueue<PendingTask<?>> queue;
        /**
         * The configured max concurrency for this group (= initial semaphore permit count).
         */
        public final int maxConcurrency;
        /**
         * The configured queue capacity; 0 means no queuing allowed (queue.offer() should
         * never be called). The actual LinkedBlockingQueue always has capacity >= 1.
         */
        public final int queueCapacity;

        GroupState(Semaphore semaphore, LinkedBlockingQueue<PendingTask<?>> queue,
                   int maxConcurrency, int queueCapacity) {
            this.semaphore = semaphore;
            this.queue = queue;
            this.maxConcurrency = maxConcurrency;
            this.queueCapacity = queueCapacity;
        }
    }

    /**
     * A task waiting to be dispatched, holding everything needed to execute and complete it.
     */
    public static final class PendingTask<T> {
        public final String groupKey;
        public final String taskId;
        public final java.util.concurrent.Callable<T> callable;
        public final CompletableFuture<GroupResult<T>> future;
        /** The global in-flight semaphore permit this task holds (null if no global limit). */
        public final Semaphore globalSemaphore;

        public PendingTask(String groupKey,
                           String taskId,
                           java.util.concurrent.Callable<T> callable,
                           CompletableFuture<GroupResult<T>> future,
                           Semaphore globalSemaphore) {
            this.groupKey = groupKey;
            this.taskId = taskId;
            this.callable = callable;
            this.future = future;
            this.globalSemaphore = globalSemaphore;
        }
    }

    private final GroupPolicy policy;
    private final ConcurrentHashMap<String, GroupState> stateByGroup = new ConcurrentHashMap<>();

    public GroupStateManager(GroupPolicy policy) {
        this.policy = policy;
    }

    /**
     * Returns (creating if necessary) the {@link GroupState} for the given group.
     * The semaphore permit count and queue capacity are fixed at first creation.
     */
    public GroupState stateFor(String groupKey) {
        return stateByGroup.computeIfAbsent(groupKey, key -> {
            int concurrency = policy.resolveConcurrency(key);
            int queueCapacity = policy.resolveQueueThreshold(key);
            Semaphore sem = new Semaphore(concurrency, true);
            // LinkedBlockingQueue requires capacity >= 1.
            // When queueCapacity == 0 we store capacity=0 in GroupState and GroupExecutor
            // checks state.queueCapacity == 0 before attempting queue.offer().
            // When queueCapacity == Integer.MAX_VALUE we create an unbounded queue.
            LinkedBlockingQueue<PendingTask<?>> queue =
                    queueCapacity == Integer.MAX_VALUE
                            ? new LinkedBlockingQueue<>()
                            : new LinkedBlockingQueue<>(Math.max(1, queueCapacity));
            return new GroupState(sem, queue, concurrency, queueCapacity);
        });
    }

    /**
     * Dispatch a task that already holds a semaphore permit.
     * Creates a virtual thread to execute the task and calls {@link #onTaskDone} in a finally block.
     */
    @SuppressWarnings("unchecked")
    public <T> void dispatch(GroupState state, PendingTask<T> task, TaskLifecycleListener listener) {
        Thread.ofVirtual().start(() -> {
            try {
                // If the future was already cancelled before we start, skip execution
                if (task.future.isCancelled()) {
                    return;
                }

                long startNanos = System.nanoTime();
                fireOnStarted(listener, task.groupKey, task.taskId);

                GroupResult<T> result;
                try {
                    T value = task.callable.call();
                    long endNanos = System.nanoTime();
                    result = GroupResult.success(task.groupKey, task.taskId, value, startNanos, endNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    long endNanos = System.nanoTime();
                    result = GroupResult.cancelled(task.groupKey, task.taskId, e, startNanos, endNanos);
                } catch (CancellationException e) {
                    long endNanos = System.nanoTime();
                    result = GroupResult.cancelled(task.groupKey, task.taskId, e, startNanos, endNanos);
                } catch (Exception e) {
                    long endNanos = System.nanoTime();
                    result = GroupResult.failed(task.groupKey, task.taskId, e, startNanos, endNanos);
                }

                fireOnCompleted(listener, task.groupKey, task.taskId, result);
                task.future.complete(result);
            } finally {
                onTaskDone(state, task.globalSemaphore, listener);
            }
        });
    }

    /**
     * Called when a task finishes execution.
     *
     * <p>Hand-off pattern: if there is a queued task, pass the permit directly to it
     * (avoiding a release/acquire round-trip). Otherwise release the permit and call
     * {@link #tryDispatch} to close the race window.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void onTaskDone(GroupState state, Semaphore globalSemaphore, TaskLifecycleListener listener) {
        // Always release the completing task's global permit — each task independently
        // acquired its own global permit at submission time.
        if (globalSemaphore != null) {
            globalSemaphore.release();
        }

        PendingTask<?> next = state.queue.poll();
        if (next != null) {
            // Hand-off: the per-group concurrency permit is transferred directly to the
            // next task (no release/acquire round-trip). The next task still holds its own
            // global permit that was acquired when it was submitted.
            dispatch(state, (PendingTask) next, listener);
        } else {
            state.semaphore.release();
            tryDispatch(state, listener);
        }
    }

    /**
     * Race-condition safety valve: called after enqueueing and after releasing the semaphore.
     *
     * <p>Handles the window where:
     * <ol>
     *   <li>Task A finishes, polls queue → empty.</li>
     *   <li>Task B submits, tryAcquire → fails (A hasn't released yet), enqueues.</li>
     *   <li>Task A releases semaphore, calls tryDispatch → finds B in queue, dispatches it.</li>
     * </ol>
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void tryDispatch(GroupState state, TaskLifecycleListener listener) {
        if (state.queue.isEmpty()) return;
        if (!state.semaphore.tryAcquire()) return;

        PendingTask<?> next = state.queue.poll();
        if (next == null) {
            // Someone else dispatched between isEmpty() check and our tryAcquire.
            state.semaphore.release();
            return;
        }
        // The global semaphore permit was already acquired when this task was enqueued.
        dispatch(state, (PendingTask) next, listener);
    }

    /**
     * Evict the state for a single group. Any tasks still in the queue are completed as REJECTED.
     */
    public void evict(String groupKey) {
        GroupState state = stateByGroup.remove(groupKey);
        if (state != null) {
            drainQueueAsRejected(state);
        }
    }

    /**
     * Evict all groups. Any queued tasks are completed as REJECTED.
     */
    public void clear() {
        stateByGroup.forEach((key, state) -> drainQueueAsRejected(state));
        stateByGroup.clear();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void drainQueueAsRejected(GroupState state) {
        PendingTask<?> task;
        while ((task = state.queue.poll()) != null) {
            GroupResult rejected = GroupResult.rejected(task.groupKey, task.taskId);
            ((PendingTask) task).future.complete(rejected);
            // Release the global semaphore permit the task was holding (if any)
            if (task.globalSemaphore != null) {
                task.globalSemaphore.release();
            }
        }
    }

    // ---- Metrics snapshot helpers ----

    /**
     * Returns a point-in-time snapshot of the state for the given group, or empty if the
     * group has no allocated state.
     */
    public Optional<GroupStats> snapshotFor(String groupKey) {
        GroupState state = stateByGroup.get(groupKey);
        if (state == null) {
            return Optional.empty();
        }
        int activeCount = Math.max(0, state.maxConcurrency - state.semaphore.availablePermits());
        int queuedCount = state.queue.size();
        return Optional.of(new GroupStats(groupKey, activeCount, queuedCount,
                state.maxConcurrency, state.queueCapacity));
    }

    /**
     * Returns an unmodifiable snapshot of the currently allocated group keys.
     */
    public Set<String> activeGroupKeys() {
        return Set.copyOf(stateByGroup.keySet());
    }

    /**
     * Aggregated totals across all groups: group count, active count, and queued count.
     */
    public record AggregateTotals(int activeGroupCount, int totalActiveCount, int totalQueuedCount) {}

    /**
     * Aggregates totals across all groups: active count, queued count, and group count.
     */
    public AggregateTotals aggregateTotals() {
        int activeGroupCount = 0;
        int totalActive = 0;
        int totalQueued = 0;
        for (GroupState state : stateByGroup.values()) {
            activeGroupCount++;
            totalActive += Math.max(0, state.maxConcurrency - state.semaphore.availablePermits());
            totalQueued += state.queue.size();
        }
        return new AggregateTotals(activeGroupCount, totalActive, totalQueued);
    }

    // ---- Listener helpers ----

    private void fireOnStarted(TaskLifecycleListener listener, String groupKey, String taskId) {
        if (listener != null) {
            try {
                listener.onStarted(groupKey, taskId);
            } catch (RuntimeException ignored) {
            }
        }
    }

    private void fireOnCompleted(TaskLifecycleListener listener, String groupKey, String taskId,
                                 GroupResult<?> result) {
        if (listener != null) {
            try {
                listener.onCompleted(groupKey, taskId, result);
            } catch (RuntimeException ignored) {
            }
        }
    }
}
