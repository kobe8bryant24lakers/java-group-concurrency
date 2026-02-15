package io.github.kobe;

/**
 * Thrown when a task is rejected because the queue threshold has been exceeded.
 */
public class RejectedTaskException extends RuntimeException {

    private final String groupKey;
    private final String taskId;

    public RejectedTaskException(String groupKey, String taskId) {
        super("Task rejected: groupKey=" + groupKey + ", taskId=" + taskId + " (queue threshold exceeded)");
        this.groupKey = groupKey;
        this.taskId = taskId;
    }

    public String groupKey() {
        return groupKey;
    }

    public String taskId() {
        return taskId;
    }
}
