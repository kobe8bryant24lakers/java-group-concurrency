package io.github.kobe;

import io.github.kobe.internal.GroupBulkheadManager;
import io.github.kobe.internal.GroupExecutorManager;
import io.github.kobe.internal.GroupQueueManager;
import io.github.kobe.internal.GroupSemaphoreManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.time.Duration;

/**
 * Executes tasks grouped by a groupKey with per-group concurrency control using virtual threads.
 */
public final class GroupExecutor implements AutoCloseable {

    private final GroupPolicy policy;
    private final GroupExecutorManager executorManager;
    private final GroupSemaphoreManager semaphoreManager;
    private final GroupBulkheadManager bulkheadManager;
    private final Semaphore globalInFlightSemaphore;
    private final Semaphore globalQueueSemaphore;       // null when no global queue threshold
    private final GroupQueueManager queueManager;        // null when no per-group queue threshold
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> groupLocks = new ConcurrentHashMap<>();

    private GroupExecutor(GroupPolicy policy,
                          GroupExecutorManager executorManager,
                          GroupSemaphoreManager semaphoreManager,
                          GroupBulkheadManager bulkheadManager,
                          Semaphore globalInFlightSemaphore,
                          Semaphore globalQueueSemaphore,
                          GroupQueueManager queueManager) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.executorManager = Objects.requireNonNull(executorManager, "executorManager");
        this.semaphoreManager = Objects.requireNonNull(semaphoreManager, "semaphoreManager");
        this.bulkheadManager = Objects.requireNonNull(bulkheadManager, "bulkheadManager");
        this.globalInFlightSemaphore = Objects.requireNonNull(globalInFlightSemaphore, "globalInFlightSemaphore");
        this.globalQueueSemaphore = globalQueueSemaphore;   // nullable
        this.queueManager = queueManager;                    // nullable
    }

    /**
     * Create a new executor backed by virtual threads with per-group isolation.
     */
    public static GroupExecutor newVirtualThreadExecutor(GroupPolicy policy) {
        Semaphore globalQueueSem = policy.globalQueueThreshold() < Integer.MAX_VALUE
                ? new Semaphore(policy.globalQueueThreshold(), true)
                : null;
        GroupQueueManager queueMgr = policy.defaultQueueThresholdPerGroup() < Integer.MAX_VALUE
                || !policy.perGroupQueueThreshold().isEmpty()
                ? new GroupQueueManager(policy)
                : null;
        return new GroupExecutor(
                policy,
                new GroupExecutorManager(),
                new GroupSemaphoreManager(policy),
                new GroupBulkheadManager(policy),
                new Semaphore(policy.globalMaxInFlight(), true),
                globalQueueSem,
                queueMgr
        );
    }

    /**
     * Submit a single task to the executor.
     */
    public <T> TaskHandle<T> submit(String groupKey, String taskId, java.util.concurrent.Callable<T> task) {
        ensureOpen();
        Objects.requireNonNull(groupKey, "groupKey");
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(task, "task");

        fireOnSubmitted(groupKey, taskId);
        ReentrantReadWriteLock.ReadLock readLock = lockFor(groupKey).readLock();
        readLock.lock();
        try {
            ExecutorService groupExecutor = executorManager.executorFor(groupKey);
            var future = groupExecutor.submit(() -> executeWithIsolation(groupKey, taskId, task));
            return new TaskHandle<>(groupKey, taskId, future);
        } finally {
            readLock.unlock();
        }
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

    private <T> GroupResult<T> executeWithIsolation(String groupKey, String taskId, java.util.concurrent.Callable<T> task) {
        boolean globalQueueAcquired = false;
        boolean globalAcquired = false;
        boolean bulkheadAcquired = false;
        boolean perGroupQueueAcquired = false;
        boolean semaphoreAcquired = false;

        // Look up per-group resources under read lock to ensure consistency with evictGroup
        Semaphore bulkhead;
        Semaphore semaphore;
        Semaphore perGroupQueue;
        ReentrantReadWriteLock.ReadLock readLock = lockFor(groupKey).readLock();
        readLock.lock();
        try {
            bulkhead = bulkheadManager.bulkheadFor(groupKey);
            semaphore = semaphoreManager.semaphoreFor(groupKey);
            perGroupQueue = queueManager != null ? queueManager.queueSemaphoreFor(groupKey) : null;
        } finally {
            readLock.unlock();
        }

        try {
            // Layer 1: global in-flight — try non-blocking first
            if (globalInFlightSemaphore.tryAcquire()) {
                globalAcquired = true;
            } else {
                // Need to wait — check global queue threshold
                if (globalQueueSemaphore != null) {
                    if (!globalQueueSemaphore.tryAcquire()) {
                        return handleRejection(groupKey, taskId, task, "global queue threshold exceeded");
                    }
                    globalQueueAcquired = true;
                }

                globalInFlightSemaphore.acquire();   // Layer 1: blocking wait
                globalAcquired = true;

                // Release global queue permit — task moved from "waiting" to "in-flight"
                if (globalQueueAcquired) {
                    globalQueueSemaphore.release();
                    globalQueueAcquired = false;
                }
            }

            // Layer 2: per-group bulkhead — try non-blocking first
            if (bulkhead.tryAcquire()) {
                bulkheadAcquired = true;
            } else {
                // Need to wait — check per-group queue threshold
                if (perGroupQueue != null) {
                    if (!perGroupQueue.tryAcquire()) {
                        // Release global permit before rejection — rejected tasks should not hold permits
                        globalInFlightSemaphore.release();
                        globalAcquired = false;
                        return handleRejection(groupKey, taskId, task, "per-group queue threshold exceeded (bulkhead)");
                    }
                    perGroupQueueAcquired = true;
                }

                bulkhead.acquire();                  // Layer 2: blocking wait
                bulkheadAcquired = true;

                // Intentionally do NOT release perGroupQueueAcquired here.
                // The task is still in a "waiting" state (now waiting for Layer 3 concurrency).
                // Releasing and re-acquiring would open a race window where another task steals
                // the queue slot, causing this task to be spuriously rejected at Layer 3 despite
                // already holding the bulkhead permit (violating monotonic progress guarantees).
                // The slot will be released once the concurrency permit is acquired below.
            }

            // Layer 3: per-group concurrency — try non-blocking first
            if (semaphore.tryAcquire()) {
                semaphoreAcquired = true;
                // Task obtained execution permit immediately — release any queue slot held from
                // a Layer 2 wait, since the task is no longer in the waiting state.
                if (perGroupQueueAcquired) {
                    perGroupQueue.release();
                    perGroupQueueAcquired = false;
                }
            } else {
                // Need to wait — check per-group queue threshold only if not already holding a
                // slot from a Layer 2 wait (reuse the existing slot to avoid double-counting).
                if (perGroupQueue != null && !perGroupQueueAcquired) {
                    if (!perGroupQueue.tryAcquire()) {
                        // Release bulkhead and global permits before rejection
                        bulkhead.release();
                        bulkheadAcquired = false;
                        globalInFlightSemaphore.release();
                        globalAcquired = false;
                        return handleRejection(groupKey, taskId, task, "per-group queue threshold exceeded");
                    }
                    perGroupQueueAcquired = true;
                }

                semaphore.acquire();             // Layer 3: blocking wait
                semaphoreAcquired = true;

                // Task obtained execution permit — release the queue slot (from either layer).
                if (perGroupQueueAcquired) {
                    perGroupQueue.release();
                    perGroupQueueAcquired = false;
                }
            }

            // Capture start time after all permits acquired — durationNanos reflects execution time only
            long startNanos = System.nanoTime();
            fireOnStarted(groupKey, taskId);
            GroupResult<T> result = executeTask(groupKey, taskId, task, startNanos);
            fireOnCompleted(groupKey, taskId, result);
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long now = System.nanoTime();
            GroupResult<T> result = GroupResult.cancelled(groupKey, taskId, e, now, now);
            fireOnCompleted(groupKey, taskId, result);
            return result;
        } finally {
            if (semaphoreAcquired) semaphore.release();
            if (perGroupQueueAcquired) perGroupQueue.release();
            if (bulkheadAcquired) bulkhead.release();
            if (globalAcquired) globalInFlightSemaphore.release();
            if (globalQueueAcquired) globalQueueSemaphore.release();
        }
    }

    private <T> GroupResult<T> handleRejection(String groupKey, String taskId,
                                                java.util.concurrent.Callable<T> task, String reason) {
        fireOnRejected(groupKey, taskId, reason);

        RejectionHandler handler = policy.rejectionHandler();
        if (handler != null) {
            return handler.onRejected(groupKey, taskId, task);
        }

        return switch (policy.rejectionPolicy()) {
            case ABORT -> throw new RejectedTaskException(groupKey, taskId);
            case DISCARD -> GroupResult.rejected(groupKey, taskId);
            case CALLER_RUNS -> {
                long startNanos = System.nanoTime();
                yield executeTask(groupKey, taskId, task, startNanos);
            }
        };
    }

    private <T> GroupResult<T> executeTask(String groupKey, String taskId, java.util.concurrent.Callable<T> task,
                                           long startNanos) {
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
     * Shut down a single group's executor without affecting other groups.
     */
    public void shutdownGroup(String groupKey) {
        executorManager.shutdownGroup(groupKey);
    }

    public void shutdown() {
        if (closed.compareAndSet(false, true)) {
            // close() calls shutdownNow() on all executors (non-blocking). Running tasks still
            // hold direct references to their semaphore objects and will release permits on
            // completion. Clearing manager maps allows GC of idle entries once all tasks finish.
            executorManager.close();
            semaphoreManager.clear();
            bulkheadManager.clear();
            if (queueManager != null) queueManager.clear();
            groupLocks.clear();
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    /**
     * Graceful shutdown with timeout. Initiates shutdown, waits up to the specified duration,
     * then forces shutdown if tasks are still running.
     *
     * @param timeout max time to wait for tasks to complete
     * @return true if all tasks completed before the timeout, false if forced shutdown was needed
     */
    public boolean shutdown(Duration timeout) {
        if (closed.compareAndSet(false, true)) {
            boolean completed = executorManager.awaitTermination(timeout);
            semaphoreManager.clear();
            bulkheadManager.clear();
            if (queueManager != null) queueManager.clear();
            groupLocks.clear();
            return completed;
        }
        return true;
    }

    /**
     * Evict all cached resources for the given group. The executor, semaphore, and bulkhead
     * for this group will be removed and recreated on next use.
     * <p>
     * Uses a per-group write lock to ensure atomicity with respect to concurrent
     * {@link #submit} and {@link #executeWithIsolation} calls for the same group.
     */
    public void evictGroup(String groupKey) {
        Objects.requireNonNull(groupKey, "groupKey");
        ReentrantReadWriteLock.WriteLock writeLock = lockFor(groupKey).writeLock();
        writeLock.lock();
        try {
            executorManager.evict(groupKey);
            semaphoreManager.evict(groupKey);
            bulkheadManager.evict(groupKey);
            if (queueManager != null) queueManager.evict(groupKey);
        } finally {
            writeLock.unlock();
        }
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

    private void fireOnStarted(String groupKey, String taskId) {
        TaskLifecycleListener listener = policy.taskLifecycleListener();
        if (listener != null) {
            try {
                listener.onStarted(groupKey, taskId);
            } catch (RuntimeException ignored) {
            }
        }
    }

    private void fireOnCompleted(String groupKey, String taskId, GroupResult<?> result) {
        TaskLifecycleListener listener = policy.taskLifecycleListener();
        if (listener != null) {
            try {
                listener.onCompleted(groupKey, taskId, result);
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
