package io.github.kobe;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Handle returned for each submitted task.
 */
public final class TaskHandle<T> {

    private final String groupKey;
    private final String taskId;
    private final Future<GroupResult<T>> future;

    TaskHandle(String groupKey, String taskId, Future<GroupResult<T>> future) {
        this.groupKey = Objects.requireNonNull(groupKey, "groupKey");
        this.taskId = Objects.requireNonNull(taskId, "taskId");
        this.future = Objects.requireNonNull(future, "future");
    }

    public String groupKey() {
        return groupKey;
    }

    public String taskId() {
        return taskId;
    }

    public Future<GroupResult<T>> future() {
        return future;
    }

    /**
     * Wait for completion and return the {@link GroupResult}.
     */
    public GroupResult<T> await() throws InterruptedException {
        try {
            return future.get();
        } catch (CancellationException e) {
            long now = System.nanoTime();
            return GroupResult.cancelled(groupKey, taskId, e, now, now);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("Task wrapper failed unexpectedly", cause);
        }
    }

    /**
     * Wait without checked exceptions, rethrowing runtime failures.
     */
    public GroupResult<T> join() {
        try {
            return await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long now = System.nanoTime();
            return GroupResult.cancelled(groupKey, taskId, e, now, now);
        }
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    public boolean isDone() {
        return future.isDone();
    }
}
