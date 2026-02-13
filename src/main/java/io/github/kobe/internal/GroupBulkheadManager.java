package io.github.kobe.internal;

import io.github.kobe.GroupPolicy;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Manages per-group bulkhead semaphores (Layer 2) to limit in-flight tasks per group.
 * Uses fair semaphores to ensure FIFO ordering and prevent starvation.
 */
public final class GroupBulkheadManager {

    private final GroupPolicy policy;
    private final ConcurrentHashMap<String, Semaphore> bulkheadByGroup = new ConcurrentHashMap<>();

    public GroupBulkheadManager(GroupPolicy policy) {
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    public Semaphore bulkheadFor(String groupKey) {
        return bulkheadByGroup.computeIfAbsent(groupKey, this::create);
    }

    private Semaphore create(String groupKey) {
        return new Semaphore(policy.resolveMaxInFlight(groupKey), true);
    }

    public void clear() {
        bulkheadByGroup.clear();
    }

    public void evict(String groupKey) {
        bulkheadByGroup.remove(groupKey);
    }
}
