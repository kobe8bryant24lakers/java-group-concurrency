package io.github.kobe.internal;

import io.github.kobe.GroupPolicy;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Manages per-group queue semaphores to limit the number of tasks waiting for
 * concurrency permits. Uses fair semaphores to ensure FIFO ordering.
 */
public final class GroupQueueManager {

    private final GroupPolicy policy;
    private final ConcurrentHashMap<String, Semaphore> queueByGroup = new ConcurrentHashMap<>();

    public GroupQueueManager(GroupPolicy policy) {
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    public Semaphore queueSemaphoreFor(String groupKey) {
        return queueByGroup.computeIfAbsent(groupKey, this::create);
    }

    private Semaphore create(String groupKey) {
        return new Semaphore(policy.resolveQueueThreshold(groupKey), true);
    }

    public void clear() {
        queueByGroup.clear();
    }

    public void evict(String groupKey) {
        queueByGroup.remove(groupKey);
    }
}
