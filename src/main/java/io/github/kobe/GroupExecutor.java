package io.github.kobe;

import io.github.kobe.internal.GroupBulkheadManager;
import io.github.kobe.internal.GroupExecutorManager;
import io.github.kobe.internal.GroupSemaphoreManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes tasks grouped by a groupKey with per-group concurrency control using virtual threads.
 */
public final class GroupExecutor implements AutoCloseable {

    private final GroupPolicy policy;
    private final GroupExecutorManager executorManager;
    private final GroupSemaphoreManager semaphoreManager;
    private final GroupBulkheadManager bulkheadManager;
    private final Semaphore globalInFlightSemaphore;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private GroupExecutor(GroupPolicy policy,
                          GroupExecutorManager executorManager,
                          GroupSemaphoreManager semaphoreManager,
                          GroupBulkheadManager bulkheadManager,
                          Semaphore globalInFlightSemaphore) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.executorManager = Objects.requireNonNull(executorManager, "executorManager");
        this.semaphoreManager = Objects.requireNonNull(semaphoreManager, "semaphoreManager");
        this.bulkheadManager = Objects.requireNonNull(bulkheadManager, "bulkheadManager");
        this.globalInFlightSemaphore = Objects.requireNonNull(globalInFlightSemaphore, "globalInFlightSemaphore");
    }

    /**
     * Create a new executor backed by virtual threads with per-group isolation.
     */
    public static GroupExecutor newVirtualThreadExecutor(GroupPolicy policy) {
        return new GroupExecutor(
                policy,
                new GroupExecutorManager(),
                new GroupSemaphoreManager(policy),
                new GroupBulkheadManager(policy),
                new Semaphore(policy.globalMaxInFlight(), true)
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

        ExecutorService groupExecutor = executorManager.executorFor(groupKey);
        var future = groupExecutor.submit(() -> executeWithIsolation(groupKey, taskId, task));
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
        long startNanos = System.nanoTime();
        boolean globalAcquired = false;
        boolean bulkheadAcquired = false;
        boolean semaphoreAcquired = false;
        Semaphore bulkhead = bulkheadManager.bulkheadFor(groupKey);
        Semaphore semaphore = semaphoreManager.semaphoreFor(groupKey);
        try {
            globalInFlightSemaphore.acquire();   // Layer 1: global in-flight
            globalAcquired = true;
            bulkhead.acquire();                  // Layer 2: per-group in-flight
            bulkheadAcquired = true;
            semaphore.acquire();                 // Layer 3: per-group concurrency
            semaphoreAcquired = true;
            return executeTask(groupKey, taskId, task, startNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long end = System.nanoTime();
            return GroupResult.cancelled(groupKey, taskId, e, startNanos, end);
        } finally {
            if (semaphoreAcquired) semaphore.release();
            if (bulkheadAcquired) bulkhead.release();
            if (globalAcquired) globalInFlightSemaphore.release();
        }
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
            executorManager.close();
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("executor is shut down");
        }
    }
}
