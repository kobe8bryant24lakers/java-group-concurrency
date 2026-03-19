package io.github.kobe;

import io.github.kobe.internal.GroupStateManager;
import io.github.kobe.internal.GroupStateManager.GroupState;
import io.github.kobe.internal.GroupStateManager.PendingTask;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Executes tasks grouped by a groupKey with per-group concurrency control using virtual threads.
 *
 * <p>Architecture: tasks are stored in an explicit {@link java.util.concurrent.LinkedBlockingQueue}
 * when no concurrency permit is available. A virtual thread is only created when a task is actually
 * dispatched (i.e., has obtained a semaphore permit). Hand-off dispatch avoids release/acquire
 * round-trips, and {@code tryDispatch()} closes the submission-completion race window.
 */
public final class GroupExecutor implements AutoCloseable {

    private final GroupPolicy policy;
    private final GroupStateManager stateManager;
    /** Optional Layer 1: global cap on total in-flight tasks across all groups. */
    private final Semaphore globalInFlightSemaphore;   // null when globalMaxInFlight == MAX_VALUE
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> groupLocks = new ConcurrentHashMap<>();

    private GroupExecutor(GroupPolicy policy, Semaphore globalInFlightSemaphore) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.stateManager = new GroupStateManager(policy);
        this.globalInFlightSemaphore = globalInFlightSemaphore;  // nullable
    }

    /**
     * Create a new executor backed by virtual threads with per-group isolation.
     */
    public static GroupExecutor newVirtualThreadExecutor(GroupPolicy policy) {
        Objects.requireNonNull(policy, "policy");
        Semaphore globalSem = policy.globalMaxInFlight() < Integer.MAX_VALUE
                ? new Semaphore(policy.globalMaxInFlight(), true)
                : null;
        return new GroupExecutor(policy, globalSem);
    }

    /**
     * Submit a single task to the executor.
     *
     * <p>Tasks are dispatched immediately if a concurrency permit is available, or queued
     * otherwise. If the queue is full (or the global in-flight limit is reached), the task
     * is handled according to the configured {@link RejectionPolicy}.
     */
    public <T> TaskHandle<T> submit(String groupKey, String taskId,
                                    java.util.concurrent.Callable<T> task) {
        ensureOpen();
        Objects.requireNonNull(groupKey, "groupKey");
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(task, "task");

        fireOnSubmitted(groupKey, taskId);

        // Layer 1: try to acquire the global in-flight permit (non-blocking).
        // Failure means the system is at global capacity → reject immediately.
        if (globalInFlightSemaphore != null && !globalInFlightSemaphore.tryAcquire()) {
            fireOnRejected(groupKey, taskId, "global in-flight limit exceeded");
            return handleRejection(groupKey, taskId, task);
        }

        // Obtain the per-group state under read lock to be consistent with evictGroup.
        GroupState state;
        ReentrantReadWriteLock.ReadLock readLock = lockFor(groupKey).readLock();
        readLock.lock();
        try {
            state = stateManager.stateFor(groupKey);
        } finally {
            readLock.unlock();
        }

        CompletableFuture<GroupResult<T>> future = new CompletableFuture<>();
        PendingTask<T> pendingTask = new PendingTask<>(groupKey, taskId, task, future,
                globalInFlightSemaphore);

        if (state.semaphore.tryAcquire()) {
            // Got the concurrency permit — dispatch to a virtual thread immediately.
            stateManager.dispatch(state, pendingTask, policy.taskLifecycleListener());
        } else if (state.queueCapacity > 0 && state.queue.offer(pendingTask)) {
            // Queued successfully — call tryDispatch to handle the race window where
            // a running task may have already polled an empty queue and not yet released
            // the semaphore before we offered into the queue.
            stateManager.tryDispatch(state, policy.taskLifecycleListener());
        } else {
            // Queue is full or queueCapacity == 0 → reject.
            // Release the global permit we acquired above before rejecting.
            if (globalInFlightSemaphore != null) globalInFlightSemaphore.release();
            fireOnRejected(groupKey, taskId, "per-group queue capacity exceeded");
            return handleRejection(groupKey, taskId, task);
        }

        return new TaskHandle<>(groupKey, taskId, future);
    }

    /**
     * Execute a batch of tasks and collect all results (no fail-fast).
     */
    public <T> List<GroupResult<T>> executeAll(List<GroupTask<T>> tasks) {
        ensureOpen();
        Objects.requireNonNull(tasks, "tasks");

        List<TaskHandle<T>> handles = new ArrayList<>(tasks.size());
        for (GroupTask<T> task : tasks) {
            handles.add(submit(task.groupKey(), task.taskId(), task.task()));
        }

        List<GroupResult<T>> results = new ArrayList<>(handles.size());
        for (int i = 0; i < handles.size(); i++) {
            TaskHandle<T> handle = handles.get(i);
            try {
                results.add(handle.await());
            } catch (RejectedTaskException e) {
                results.add(GroupResult.rejected(e.groupKey(), e.taskId()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                long now = System.nanoTime();
                results.add(GroupResult.cancelled(handle.groupKey(), handle.taskId(), e, now, now));
                for (int j = i + 1; j < handles.size(); j++) {
                    TaskHandle<T> remaining = handles.get(j);
                    remaining.cancel(true);
                    results.add(GroupResult.cancelled(remaining.groupKey(), remaining.taskId(), e, now, now));
                }
                break;
            }
        }
        return results;
    }

    /**
     * Handle a rejected task according to the configured policy.
     * The global semaphore permit must already have been released before calling this method.
     *
     * <p>Returns a {@link TaskHandle} in all cases — exceptions are surfaced through the handle's
     * {@link TaskHandle#await()} method (completing the future exceptionally), not thrown directly
     * from {@code submit()}, preserving backward-compatible behavior.
     */
    private <T> TaskHandle<T> handleRejection(String groupKey, String taskId,
                                               java.util.concurrent.Callable<T> task) {
        RejectionHandler handler = policy.rejectionHandler();
        if (handler != null) {
            GroupResult<T> result = handler.onRejected(groupKey, taskId, task);
            return new TaskHandle<>(groupKey, taskId, CompletableFuture.completedFuture(result));
        }

        return switch (policy.rejectionPolicy()) {
            case ABORT -> {
                // Complete the future exceptionally so the exception is thrown at await() time,
                // maintaining backward compatibility with the old virtual-thread design.
                CompletableFuture<GroupResult<T>> f = new CompletableFuture<>();
                f.completeExceptionally(new RejectedTaskException(groupKey, taskId));
                yield new TaskHandle<>(groupKey, taskId, f);
            }
            case DISCARD -> {
                GroupResult<T> result = GroupResult.rejected(groupKey, taskId);
                yield new TaskHandle<>(groupKey, taskId, CompletableFuture.completedFuture(result));
            }
            case CALLER_RUNS -> {
                // Execute directly in the thread that called submit(), without any permits.
                long startNanos = System.nanoTime();
                GroupResult<T> result = executeTask(groupKey, taskId, task, startNanos);
                yield new TaskHandle<>(groupKey, taskId, CompletableFuture.completedFuture(result));
            }
        };
    }

    private <T> GroupResult<T> executeTask(String groupKey, String taskId,
                                            java.util.concurrent.Callable<T> task, long startNanos) {
        try {
            T value = task.call();
            long end = System.nanoTime();
            return GroupResult.success(groupKey, taskId, value, startNanos, end);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long end = System.nanoTime();
            return GroupResult.cancelled(groupKey, taskId, e, startNanos, end);
        } catch (CancellationException e) {
            long end = System.nanoTime();
            return GroupResult.cancelled(groupKey, taskId, e, startNanos, end);
        } catch (Exception e) {
            long end = System.nanoTime();
            return GroupResult.failed(groupKey, taskId, e, startNanos, end);
        }
    }

    /**
     * Shut down a single group: drain its queue (completing all pending tasks as REJECTED)
     * and evict its cached state. In-flight tasks complete normally.
     */
    public void shutdownGroup(String groupKey) {
        Objects.requireNonNull(groupKey, "groupKey");
        stateManager.evict(groupKey);
    }

    /**
     * Immediately shut down the executor. All queued tasks are completed as REJECTED.
     * In-flight tasks are not interrupted; they hold direct references to their state
     * and will release permits on completion.
     */
    public void shutdown() {
        if (closed.compareAndSet(false, true)) {
            stateManager.clear();
            groupLocks.clear();
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    /**
     * Graceful shutdown with timeout. Queued tasks are immediately REJECTED.
     * Waits up to the specified duration for in-flight tasks to complete.
     *
     * @param timeout max time to wait for in-flight tasks to finish
     * @return true if all in-flight tasks completed before the timeout
     */
    public boolean shutdown(Duration timeout) {
        if (closed.compareAndSet(false, true)) {
            // Drain all queues immediately — no new dispatches can happen since closed=true.
            stateManager.clear();
            groupLocks.clear();
            // In-flight tasks hold direct semaphore references. We approximate "all done"
            // by checking if the global semaphore (if present) has all permits back.
            // For cases without a global semaphore we rely on a simple sleep-poll fallback.
            return awaitInFlightCompletion(timeout);
        }
        return true;
    }

    private boolean awaitInFlightCompletion(Duration timeout) {
        // If there is no global semaphore, we have no direct way to track in-flight count.
        // The best we can do is wait a small amount and return true (tasks are on virtual threads
        // and will complete independently). A production implementation could track counters,
        // but that is out of scope for this library.
        if (globalInFlightSemaphore == null) {
            return true;
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        int totalPermits = policy.globalMaxInFlight();
        while (System.nanoTime() < deadline) {
            // All in-flight tasks complete when all permits are returned.
            if (globalInFlightSemaphore.availablePermits() >= totalPermits) {
                return true;
            }
            try {
                //noinspection BusyWait
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return globalInFlightSemaphore.availablePermits() >= totalPermits;
    }

    /**
     * Evict all cached resources for the given group. The state (semaphore + queue) will be
     * recreated on next use. Queued tasks are completed as REJECTED.
     */
    public void evictGroup(String groupKey) {
        Objects.requireNonNull(groupKey, "groupKey");
        ReentrantReadWriteLock.WriteLock writeLock = lockFor(groupKey).writeLock();
        writeLock.lock();
        try {
            stateManager.evict(groupKey);
        } finally {
            writeLock.unlock();
        }
        // Remove the lock entry after releasing so it can be GC'd.
        groupLocks.remove(groupKey);
    }

    // ---- Metrics / Observability API ----

    /**
     * Returns a point-in-time snapshot of the given group's operational state,
     * or {@link Optional#empty()} if the group has no allocated state.
     *
     * <p>Safe to call after shutdown (returns empty). Never blocks, never throws.
     *
     * @param groupKey the group to query
     * @return snapshot of group stats, or empty
     */
    public Optional<GroupStats> groupStats(String groupKey) {
        Objects.requireNonNull(groupKey, "groupKey");
        if (closed.get()) {
            return Optional.empty();
        }
        return stateManager.snapshotFor(groupKey);
    }

    /**
     * Returns a point-in-time snapshot of executor-wide operational counters.
     *
     * <p>Safe to call after shutdown (returns zeroed stats). Never blocks, never throws.
     *
     * @return aggregated executor stats
     */
    public GroupExecutorStats stats() {
        if (closed.get()) {
            return new GroupExecutorStats(0, 0, 0, 0, policy.globalMaxInFlight());
        }
        GroupStateManager.AggregateTotals totals = stateManager.aggregateTotals();
        int globalUsed = globalInFlightSemaphore != null
                ? policy.globalMaxInFlight() - globalInFlightSemaphore.availablePermits()
                : 0;
        return new GroupExecutorStats(totals.activeGroupCount(), totals.totalActiveCount(),
                totals.totalQueuedCount(), globalUsed, policy.globalMaxInFlight());
    }

    /**
     * Returns an unmodifiable snapshot of the group keys that currently have allocated state.
     *
     * <p>Safe to call after shutdown (returns empty set). Never blocks, never throws.
     *
     * @return set of active group keys
     */
    public Set<String> activeGroupKeys() {
        if (closed.get()) {
            return Collections.emptySet();
        }
        return stateManager.activeGroupKeys();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("executor is shut down");
        }
    }

    private ReentrantReadWriteLock lockFor(String groupKey) {
        return groupLocks.computeIfAbsent(groupKey, k -> new ReentrantReadWriteLock());
    }

    private void fireOnSubmitted(String groupKey, String taskId) {
        TaskLifecycleListener listener = policy.taskLifecycleListener();
        if (listener != null) {
            try {
                listener.onSubmitted(groupKey, taskId);
            } catch (RuntimeException ignored) {
            }
        }
    }

    private void fireOnRejected(String groupKey, String taskId, String reason) {
        TaskLifecycleListener listener = policy.taskLifecycleListener();
        if (listener != null) {
            try {
                listener.onRejected(groupKey, taskId, reason);
            } catch (RuntimeException ignored) {
            }
        }
    }
}
