package io.github.kobe;

import java.util.concurrent.Callable;

/**
 * Custom handler invoked when a task is rejected due to queue threshold exceeded.
 * When configured, takes priority over {@link RejectionPolicy}.
 */
@FunctionalInterface
public interface RejectionHandler {

    /**
     * Handle a rejected task and produce a result.
     *
     * @param groupKey the group key of the rejected task
     * @param taskId   the task id of the rejected task
     * @param task     the original callable
     * @param <T>      the result type
     * @return the result to use for this task
     */
    <T> GroupResult<T> onRejected(String groupKey, String taskId, Callable<T> task);
}
