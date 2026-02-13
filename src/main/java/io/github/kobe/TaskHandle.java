package io.github.kobe;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
     * Wait for completion with a timeout. If the timeout expires, returns a CANCELLED result
     * with a {@link TimeoutException} as the error. Does <b>not</b> cancel the underlying task.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return the task result or a CANCELLED result on timeout
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public GroupResult<T> await(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            return future.get(timeout, unit);
        } catch (TimeoutException e) {
            long now = System.nanoTime();
            return GroupResult.cancelled(groupKey, taskId, e, now, now);
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

    /**
     * Wait with a timeout without checked exceptions. If the timeout expires, returns a CANCELLED
     * result with a {@link TimeoutException} as the error. Does <b>not</b> cancel the underlying task.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return the task result or a CANCELLED result on timeout/interruption
     */
    public GroupResult<T> join(long timeout, TimeUnit unit) {
        try {
            return await(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long now = System.nanoTime();
            return GroupResult.cancelled(groupKey, taskId, e, now, now);
        }
    }

    /**
     * Bridge this handle to a {@link CompletableFuture}. The returned future is completed
     * via a virtual thread that waits for the underlying task to finish.
     *
     * @return a CompletableFuture that completes with the GroupResult
     */
    public CompletableFuture<GroupResult<T>> toCompletableFuture() {
        CompletableFuture<GroupResult<T>> cf = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                cf.complete(await());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                long now = System.nanoTime();
                cf.complete(GroupResult.cancelled(groupKey, taskId, e, now, now));
            } catch (RuntimeException e) {
                cf.completeExceptionally(e);
            }
        });
        return cf;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    public boolean isDone() {
        return future.isDone();
    }
}
