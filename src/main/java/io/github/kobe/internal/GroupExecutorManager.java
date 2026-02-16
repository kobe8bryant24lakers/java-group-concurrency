package io.github.kobe.internal;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Manages per-group virtual thread executors for fault isolation.
 * Each group gets its own {@link ExecutorService} backed by virtual threads.
 */
public final class GroupExecutorManager implements AutoCloseable {

    private final ConcurrentHashMap<String, ExecutorService> executorByGroup = new ConcurrentHashMap<>();

    public ExecutorService executorFor(String groupKey) {
        return executorByGroup.computeIfAbsent(groupKey,
                k -> Executors.newVirtualThreadPerTaskExecutor());
    }

    public void shutdownGroup(String groupKey) {
        executorByGroup.compute(groupKey, (key, existing) -> {
            if (existing != null) existing.shutdownNow();
            return null;
        });
    }

    public void evict(String groupKey) {
        shutdownGroup(groupKey);
    }

    /**
     * Graceful shutdown with timeout: first call shutdown() on all executors, wait up to timeout,
     * then shutdownNow() on any that did not terminate.
     * <p>
     * Note: callers must set a "closed" flag before invoking this method to prevent concurrent
     * {@link #executorFor} calls from adding new executors that would miss the shutdown.
     *
     * @return true if all executors terminated within the timeout
     */
    public boolean awaitTermination(Duration timeout) {
        executorByGroup.values().forEach(ExecutorService::shutdown);
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        boolean allTerminated = true;
        for (ExecutorService executor : executorByGroup.values()) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                allTerminated = false;
                executor.shutdownNow();
            } else {
                try {
                    if (!executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                        allTerminated = false;
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    allTerminated = false;
                    executor.shutdownNow();
                }
            }
        }
        executorByGroup.clear();
        return allTerminated;
    }

    @Override
    public void close() {
        executorByGroup.values().forEach(ExecutorService::shutdownNow);
        executorByGroup.clear();
    }
}
