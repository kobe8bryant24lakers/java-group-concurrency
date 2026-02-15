package io.github.kobe;

/**
 * Listener for task lifecycle events. All methods have default no-op implementations.
 * Exceptions thrown by listener methods are silently caught and ignored.
 */
public interface TaskLifecycleListener {

    /**
     * Called when a task is submitted to the executor.
     */
    default void onSubmitted(String groupKey, String taskId) {}

    /**
     * Called when a task starts execution (after acquiring all semaphore permits).
     */
    default void onStarted(String groupKey, String taskId) {}

    /**
     * Called when a task completes (successfully, with failure, or cancelled).
     */
    default void onCompleted(String groupKey, String taskId, GroupResult<?> result) {}

    /**
     * Called when a task is rejected because the queue threshold has been exceeded.
     *
     * @param groupKey the group key of the rejected task
     * @param taskId   the task id of the rejected task
     * @param reason   description of why the task was rejected
     */
    default void onRejected(String groupKey, String taskId, String reason) {}
}
