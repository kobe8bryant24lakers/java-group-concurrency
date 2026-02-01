package io.github.kobe;

/**
 * Execution outcome of a task submitted through {@link GroupExecutor}.
 *
 * @param groupKey        group identifier
 * @param taskId          caller supplied id
 * @param status          SUCCESS / FAILED / CANCELLED
 * @param value           result when successful, otherwise {@code null}
 * @param error           failure or cancellation cause, otherwise {@code null}
 * @param startTimeNanos  start timestamp from {@link System#nanoTime()}
 * @param endTimeNanos    end timestamp from {@link System#nanoTime()}
 * @param <T>             result type
 */
public record GroupResult<T>(
        String groupKey,
        String taskId,
        TaskStatus status,
        T value,
        Throwable error,
        long startTimeNanos,
        long endTimeNanos
) {

    public long durationNanos() {
        return endTimeNanos - startTimeNanos;
    }

    public static <T> GroupResult<T> success(String groupKey, String taskId, T value,
                                             long startTimeNanos, long endTimeNanos) {
        return new GroupResult<>(groupKey, taskId, TaskStatus.SUCCESS, value, null, startTimeNanos, endTimeNanos);
    }

    public static <T> GroupResult<T> failed(String groupKey, String taskId, Throwable error,
                                            long startTimeNanos, long endTimeNanos) {
        return new GroupResult<>(groupKey, taskId, TaskStatus.FAILED, null, error, startTimeNanos, endTimeNanos);
    }

    public static <T> GroupResult<T> cancelled(String groupKey, String taskId, Throwable error,
                                               long startTimeNanos, long endTimeNanos) {
        return new GroupResult<>(groupKey, taskId, TaskStatus.CANCELLED, null, error, startTimeNanos, endTimeNanos);
    }
}
