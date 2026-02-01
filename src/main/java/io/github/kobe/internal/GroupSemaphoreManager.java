package io.github.kobe.internal;

import io.github.kobe.GroupPolicy;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Lazily creates and caches Semaphores per group based on the provided policy.
 * Semaphores are fixed at first creation to keep the policy simple and predictable.
 */
public final class GroupSemaphoreManager {

    private final GroupPolicy policy;
    private final ConcurrentHashMap<String, Semaphore> semaphoreByGroup = new ConcurrentHashMap<>();

    public GroupSemaphoreManager(GroupPolicy policy) {
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    public Semaphore semaphoreFor(String groupKey) {
        return semaphoreByGroup.computeIfAbsent(groupKey, this::createSemaphore);
    }

    private Semaphore createSemaphore(String groupKey) {
        int permits = policy.resolveConcurrency(groupKey);
        return new Semaphore(permits);
    }

    public int knownGroupCount() {
        return semaphoreByGroup.size();
    }
}
