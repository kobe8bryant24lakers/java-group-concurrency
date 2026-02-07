package io.github.kobe.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        ExecutorService ex = executorByGroup.remove(groupKey);
        if (ex != null) {
            ex.shutdownNow();
        }
    }

    @Override
    public void close() {
        executorByGroup.values().forEach(ExecutorService::shutdown);
    }
}
