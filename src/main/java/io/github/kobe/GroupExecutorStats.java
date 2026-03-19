package io.github.kobe;

/**
 * Point-in-time snapshot of executor-wide operational counters.
 *
 * <p>All counters reflect instantaneous values aggregated from per-group state and
 * the global in-flight semaphore. Because tasks are dispatched and completed
 * concurrently, the values may be slightly stale by the time the caller inspects them.
 *
 * @param activeGroupCount  number of groups with allocated state
 * @param totalActiveCount  sum of active (executing) tasks across all groups
 * @param totalQueuedCount  sum of queued (waiting) tasks across all groups
 * @param globalInFlightUsed global permits currently held (0 if no global limit)
 * @param globalInFlightMax  configured globalMaxInFlight value
 */
public record GroupExecutorStats(
        int activeGroupCount,
        int totalActiveCount,
        int totalQueuedCount,
        int globalInFlightUsed,
        int globalInFlightMax
) {
}
