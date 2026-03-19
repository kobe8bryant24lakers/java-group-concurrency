package io.github.kobe;

/**
 * Point-in-time snapshot of a single group's operational state.
 *
 * <p>All counters reflect instantaneous values read from the underlying semaphore
 * and queue. Because tasks are dispatched and completed concurrently, the values
 * may be slightly stale by the time the caller inspects them.
 *
 * @param groupKey       the group identifier
 * @param activeCount    tasks currently executing (= maxConcurrency - availablePermits)
 * @param queuedCount    tasks waiting in the queue
 * @param maxConcurrency configured concurrency limit for this group
 * @param queueCapacity  configured queue capacity for this group
 */
public record GroupStats(
        String groupKey,
        int activeCount,
        int queuedCount,
        int maxConcurrency,
        int queueCapacity
) {
}
