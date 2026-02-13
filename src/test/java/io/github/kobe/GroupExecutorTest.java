package io.github.kobe;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroupExecutorTest {

    @Test
    void testMaxConcurrencyPerGroup() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .perGroupMaxConcurrency(Map.of("group-1", 2))
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            int taskCount = 8;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(taskCount);
            AtomicInteger current = new AtomicInteger();
            AtomicInteger maxSeen = new AtomicInteger();

            List<TaskHandle<Void>> handles = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                String taskId = "t-" + i;
                handles.add(executor.submit("group-1", taskId, () -> {
                    start.await();
                    int inFlight = current.incrementAndGet();
                    maxSeen.accumulateAndGet(inFlight, Math::max);
                    Thread.sleep(80);
                    current.decrementAndGet();
                    done.countDown();
                    return null;
                }));
            }

            start.countDown();
            done.await();
            for (TaskHandle<Void> handle : handles) {
                assertEquals(TaskStatus.SUCCESS, handle.await().status());
            }
            assertTrue(maxSeen.get() <= 2, "group concurrency should be capped at 2");
        }
    }

    @Test
    void testParallelAcrossGroups() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .perGroupMaxConcurrency(Map.of("A", 2, "B", 2))
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(4);
            AtomicInteger totalCurrent = new AtomicInteger();
            AtomicInteger totalMax = new AtomicInteger();

            List<TaskHandle<Void>> handles = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                int idx = i;
                handles.add(executor.submit("A", "A-" + idx, () -> {
                    start.await();
                    int running = totalCurrent.incrementAndGet();
                    totalMax.accumulateAndGet(running, Math::max);
                    Thread.sleep(100);
                    totalCurrent.decrementAndGet();
                    done.countDown();
                    return null;
                }));
                handles.add(executor.submit("B", "B-" + idx, () -> {
                    start.await();
                    int running = totalCurrent.incrementAndGet();
                    totalMax.accumulateAndGet(running, Math::max);
                    Thread.sleep(100);
                    totalCurrent.decrementAndGet();
                    done.countDown();
                    return null;
                }));
            }

            start.countDown();
            done.await();
            for (TaskHandle<Void> handle : handles) {
                assertEquals(TaskStatus.SUCCESS, handle.await().status());
            }
            assertTrue(totalMax.get() > 2, "groups should run in parallel so overall peak > 2");
        }
    }

    @Test
    void testCollectFailures() {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(2)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            List<GroupTask<String>> tasks = List.of(
                    new GroupTask<>("g", "ok-1", () -> "ok"),
                    new GroupTask<>("g", "fail-1", () -> {
                        throw new IllegalStateException("boom");
                    }),
                    new GroupTask<>("g", "ok-2", () -> "ok-2")
            );

            List<GroupResult<String>> results = executor.executeAll(tasks);

            assertEquals(3, results.size());
            assertEquals(TaskStatus.SUCCESS, results.get(0).status());
            assertEquals("ok", results.get(0).value());

            assertEquals(TaskStatus.FAILED, results.get(1).status());
            assertTrue(results.get(1).error() instanceof IllegalStateException);

            assertEquals(TaskStatus.SUCCESS, results.get(2).status());
            assertEquals("ok-2", results.get(2).value());
        }
    }

    @Test
    void testDynamicConcurrencyResolver() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .concurrencyResolver(key -> key.startsWith("vip:") ? 4 : 1)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            int vipTasks = 8;
            int regularTasks = 4;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(vipTasks + regularTasks);

            AtomicInteger vipCurrent = new AtomicInteger();
            AtomicInteger vipMax = new AtomicInteger();
            AtomicInteger regularCurrent = new AtomicInteger();
            AtomicInteger regularMax = new AtomicInteger();

            List<TaskHandle<Void>> handles = new ArrayList<>();
            for (int i = 0; i < vipTasks; i++) {
                String taskId = "vip-" + i;
                handles.add(executor.submit("vip:alpha", taskId, () -> {
                    start.await();
                    int running = vipCurrent.incrementAndGet();
                    vipMax.accumulateAndGet(running, Math::max);
                    Thread.sleep(80);
                    vipCurrent.decrementAndGet();
                    done.countDown();
                    return null;
                }));
            }

            for (int i = 0; i < regularTasks; i++) {
                String taskId = "user-" + i;
                handles.add(executor.submit("user:standard", taskId, () -> {
                    start.await();
                    int running = regularCurrent.incrementAndGet();
                    regularMax.accumulateAndGet(running, Math::max);
                    Thread.sleep(80);
                    regularCurrent.decrementAndGet();
                    done.countDown();
                    return null;
                }));
            }

            start.countDown();
            done.await();
            for (TaskHandle<Void> handle : handles) {
                assertEquals(TaskStatus.SUCCESS, handle.await().status());
            }

            assertTrue(vipMax.get() <= 4, "vip concurrency should respect resolver (<=4)");
            assertTrue(vipMax.get() > 1, "vip group should allow higher concurrency than default");
            assertEquals(1, regularMax.get(), "default concurrency should be 1 for regular group");
        }
    }

    @Test
    void testPerGroupInFlightBulkhead() throws Exception {
        // Layer 2: per-group in-flight bulkhead limits total tasks queued + running per group
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(4)
                .defaultMaxInFlightPerGroup(3) // Only 3 tasks can be in-flight per group
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            int taskCount = 6;
            CountDownLatch gate = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(taskCount);
            AtomicInteger inFlight = new AtomicInteger();
            AtomicInteger maxInFlight = new AtomicInteger();

            List<TaskHandle<Void>> handles = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                handles.add(executor.submit("bulkhead-group", "t-" + i, () -> {
                    gate.await();
                    int current = inFlight.incrementAndGet();
                    maxInFlight.accumulateAndGet(current, Math::max);
                    Thread.sleep(80);
                    inFlight.decrementAndGet();
                    done.countDown();
                    return null;
                }));
            }

            gate.countDown();
            done.await();
            for (TaskHandle<Void> handle : handles) {
                assertEquals(TaskStatus.SUCCESS, handle.await().status());
            }
            assertTrue(maxInFlight.get() <= 3,
                    "per-group in-flight should be capped at 3 but was " + maxInFlight.get());
        }
    }

    @Test
    void testGlobalInFlightFairness() throws Exception {
        // Layer 1: global in-flight limit ensures total tasks across all groups is bounded
        int globalLimit = 4;
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(10)
                .globalMaxInFlight(globalLimit)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            int tasksPerGroup = 4;
            int groupCount = 3;
            int totalTasks = tasksPerGroup * groupCount;
            CountDownLatch gate = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(totalTasks);
            AtomicInteger totalInFlight = new AtomicInteger();
            AtomicInteger maxTotalInFlight = new AtomicInteger();

            List<TaskHandle<Void>> handles = new ArrayList<>();
            for (int g = 0; g < groupCount; g++) {
                String group = "group-" + g;
                for (int t = 0; t < tasksPerGroup; t++) {
                    handles.add(executor.submit(group, group + "-t-" + t, () -> {
                        gate.await();
                        int current = totalInFlight.incrementAndGet();
                        maxTotalInFlight.accumulateAndGet(current, Math::max);
                        Thread.sleep(60);
                        totalInFlight.decrementAndGet();
                        done.countDown();
                        return null;
                    }));
                }
            }

            gate.countDown();
            done.await();
            for (TaskHandle<Void> handle : handles) {
                assertEquals(TaskStatus.SUCCESS, handle.await().status());
            }
            assertTrue(maxTotalInFlight.get() <= globalLimit,
                    "global in-flight should be capped at " + globalLimit + " but was " + maxTotalInFlight.get());
        }
    }

    @Test
    void testShutdownGroupDoesNotAffectOtherGroups() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(2)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            // Submit tasks to group A
            CountDownLatch groupAStarted = new CountDownLatch(1);
            CountDownLatch groupABlock = new CountDownLatch(1);
            TaskHandle<String> handleA = executor.submit("A", "a-1", () -> {
                groupAStarted.countDown();
                groupABlock.await();
                return "A-done";
            });

            // Wait for group A task to start
            groupAStarted.await();

            // Shutdown group B — should not affect group A
            executor.shutdownGroup("B");

            // Group A task should still complete
            groupABlock.countDown();
            GroupResult<String> resultA = handleA.await();
            assertEquals(TaskStatus.SUCCESS, resultA.status());
            assertEquals("A-done", resultA.value());

            // Should be able to submit new tasks to group B (new executor created)
            TaskHandle<String> handleB = executor.submit("B", "b-1", () -> "B-done");
            GroupResult<String> resultB = handleB.await();
            assertEquals(TaskStatus.SUCCESS, resultB.status());
            assertEquals("B-done", resultB.value());
        }
    }

    @Test
    void testUnlimitedDefaultsBehaveLikeOriginal() throws Exception {
        // With no isolation config, behavior should match original implementation
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(2)
                .build();

        // Verify defaults are Integer.MAX_VALUE
        assertEquals(Integer.MAX_VALUE, policy.globalMaxInFlight());
        assertEquals(Integer.MAX_VALUE, policy.defaultMaxInFlightPerGroup());
        assertTrue(policy.perGroupMaxInFlight().isEmpty());

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            int taskCount = 6;
            CountDownLatch gate = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(taskCount);
            AtomicInteger current = new AtomicInteger();
            AtomicInteger maxSeen = new AtomicInteger();

            List<TaskHandle<Void>> handles = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                handles.add(executor.submit("g", "t-" + i, () -> {
                    gate.await();
                    int inFlight = current.incrementAndGet();
                    maxSeen.accumulateAndGet(inFlight, Math::max);
                    Thread.sleep(80);
                    current.decrementAndGet();
                    done.countDown();
                    return null;
                }));
            }

            gate.countDown();
            done.await();
            for (TaskHandle<Void> handle : handles) {
                assertEquals(TaskStatus.SUCCESS, handle.await().status());
            }
            // Concurrency should still be limited by Layer 3 (maxConcurrency=2)
            assertTrue(maxSeen.get() <= 2,
                    "concurrency should be capped at 2 by Layer 3, but was " + maxSeen.get());
        }
    }

    // ==================== Phase 2: New Tests ====================

    @Test
    void testSubmitAfterShutdownThrows() {
        GroupPolicy policy = GroupPolicy.builder().build();
        GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy);
        executor.shutdown();

        assertThrows(IllegalStateException.class, () ->
                executor.submit("g", "t-1", () -> "value"));
    }

    @Test
    void testExecuteAllAfterShutdownThrows() {
        GroupPolicy policy = GroupPolicy.builder().build();
        GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy);
        executor.shutdown();

        List<GroupTask<String>> tasks = List.of(new GroupTask<>("g", "t-1", () -> "value"));
        assertThrows(IllegalStateException.class, () -> executor.executeAll(tasks));
    }

    @Test
    void testMultipleCloseIsIdempotent() {
        GroupPolicy policy = GroupPolicy.builder().build();
        GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy);

        assertDoesNotThrow(() -> {
            executor.close();
            executor.close();
            executor.close();
        });
    }

    @Test
    void testCancelInterruptsBlockedTask() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch taskStarted = new CountDownLatch(1);
            TaskHandle<String> handle = executor.submit("g", "blocked-task", () -> {
                taskStarted.countDown();
                Thread.sleep(10_000); // block for a long time
                return "should not reach";
            });

            taskStarted.await(2, TimeUnit.SECONDS);
            handle.cancel(true);

            GroupResult<String> result = handle.await();
            assertEquals(TaskStatus.CANCELLED, result.status());
        }
    }

    @Test
    void testResolverReturningZeroCoercedToOne() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .concurrencyResolver(key -> 0)
                .build();

        assertEquals(1, policy.resolveConcurrency("any-group"));

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            TaskHandle<String> handle = executor.submit("any-group", "t-1", () -> "ok");
            GroupResult<String> result = handle.await();
            assertEquals(TaskStatus.SUCCESS, result.status());
            assertEquals("ok", result.value());
        }
    }

    @Test
    void testResolverReturningNegativeCoercedToOne() {
        GroupPolicy policy = GroupPolicy.builder()
                .concurrencyResolver(key -> -5)
                .build();

        assertEquals(1, policy.resolveConcurrency("test-group"));
    }

    @Test
    void testResolverThrowingFallsBackToDefault() {
        GroupPolicy policy = GroupPolicy.builder()
                .concurrencyResolver(key -> {
                    throw new RuntimeException("resolver error");
                })
                .defaultMaxConcurrencyPerGroup(3)
                .build();

        assertEquals(3, policy.resolveConcurrency("any-group"));
    }

    @Test
    void testGroupTaskNullValidation() {
        assertThrows(NullPointerException.class, () -> new GroupTask<>(null, "t", () -> "v"));
        assertThrows(NullPointerException.class, () -> new GroupTask<>("g", null, () -> "v"));
        assertThrows(NullPointerException.class, () -> new GroupTask<>("g", "t", null));
    }

    @Test
    void testBuilderDefensiveCopy() throws Exception {
        HashMap<String, Integer> mutableMap = new HashMap<>();
        mutableMap.put("g1", 3);

        GroupPolicy policy = GroupPolicy.builder()
                .perGroupMaxConcurrency(mutableMap)
                .build();

        // Mutate the original map after build
        mutableMap.put("g1", 999);
        mutableMap.put("g2", 5);

        // Policy should not be affected by mutation
        assertEquals(3, policy.resolveConcurrency("g1"));
        // g2 should fall back to default (1), not 5
        assertEquals(1, policy.resolveConcurrency("g2"));

        // Also test perGroupMaxInFlight defensive copy
        HashMap<String, Integer> mutableInFlight = new HashMap<>();
        mutableInFlight.put("g1", 10);

        GroupPolicy policy2 = GroupPolicy.builder()
                .perGroupMaxInFlight(mutableInFlight)
                .build();

        mutableInFlight.put("g1", 999);
        assertEquals(10, policy2.resolveMaxInFlight("g1"));
    }

    @Test
    void testHighConcurrencyStress() throws Exception {
        int groupCount = 10;
        int tasksPerGroup = 100;
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(5)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            List<TaskHandle<String>> handles = new ArrayList<>();
            AtomicInteger completedCount = new AtomicInteger();

            for (int g = 0; g < groupCount; g++) {
                String group = "stress-group-" + g;
                for (int t = 0; t < tasksPerGroup; t++) {
                    String taskId = group + "-task-" + t;
                    handles.add(executor.submit(group, taskId, () -> {
                        Thread.sleep(1); // minimal work
                        completedCount.incrementAndGet();
                        return "done";
                    }));
                }
            }

            // Wait for all tasks to complete
            int successCount = 0;
            for (TaskHandle<String> handle : handles) {
                GroupResult<String> result = handle.await();
                if (result.status() == TaskStatus.SUCCESS) {
                    successCount++;
                }
            }

            assertEquals(groupCount * tasksPerGroup, successCount,
                    "all tasks should complete successfully");
            assertEquals(groupCount * tasksPerGroup, completedCount.get());
        }
    }

    // ==================== Phase 3: Feature Tests ====================

    @Test
    void testTaskHandleAwaitWithTimeout() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            TaskHandle<String> handle = executor.submit("g", "slow-task", () -> {
                Thread.sleep(5_000);
                return "done";
            });

            // Timeout should produce CANCELLED result with TimeoutException
            GroupResult<String> result = handle.await(50, TimeUnit.MILLISECONDS);
            assertEquals(TaskStatus.CANCELLED, result.status());
            assertInstanceOf(TimeoutException.class, result.error());

            // The underlying task should still be running (not cancelled)
            // Cancel it to clean up
            handle.cancel(true);
        }
    }

    @Test
    void testTaskHandleJoinWithTimeout() {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            TaskHandle<String> handle = executor.submit("g", "slow-task", () -> {
                Thread.sleep(5_000);
                return "done";
            });

            GroupResult<String> result = handle.join(50, TimeUnit.MILLISECONDS);
            assertEquals(TaskStatus.CANCELLED, result.status());
            assertInstanceOf(TimeoutException.class, result.error());

            handle.cancel(true);
        }
    }

    @Test
    void testToCompletableFuture() throws Exception {
        GroupPolicy policy = GroupPolicy.builder().build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            TaskHandle<String> handle = executor.submit("g", "cf-task", () -> "hello");
            CompletableFuture<GroupResult<String>> cf = handle.toCompletableFuture();

            GroupResult<String> result = cf.get(5, TimeUnit.SECONDS);
            assertEquals(TaskStatus.SUCCESS, result.status());
            assertEquals("hello", result.value());
        }
    }

    @Test
    void testEvictGroup() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            // Submit and complete a task for group "evict-test"
            TaskHandle<String> handle1 = executor.submit("evict-test", "t-1", () -> "first");
            assertEquals(TaskStatus.SUCCESS, handle1.await().status());

            // Evict the group
            executor.evictGroup("evict-test");

            // New tasks should still work (resources recreated)
            TaskHandle<String> handle2 = executor.submit("evict-test", "t-2", () -> "second");
            GroupResult<String> result = handle2.await();
            assertEquals(TaskStatus.SUCCESS, result.status());
            assertEquals("second", result.value());
        }
    }

    @Test
    void testTaskLifecycleListener() throws Exception {
        AtomicInteger submitted = new AtomicInteger();
        AtomicInteger started = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        AtomicBoolean completedSuccess = new AtomicBoolean(false);

        TaskLifecycleListener listener = new TaskLifecycleListener() {
            @Override
            public void onSubmitted(String groupKey, String taskId) {
                submitted.incrementAndGet();
            }

            @Override
            public void onStarted(String groupKey, String taskId) {
                started.incrementAndGet();
            }

            @Override
            public void onCompleted(String groupKey, String taskId, GroupResult<?> result) {
                completed.incrementAndGet();
                if (result.status() == TaskStatus.SUCCESS) {
                    completedSuccess.set(true);
                }
            }
        };

        GroupPolicy policy = GroupPolicy.builder()
                .taskLifecycleListener(listener)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            TaskHandle<String> handle = executor.submit("g", "t-1", () -> "ok");
            handle.await();
        }

        assertEquals(1, submitted.get());
        assertEquals(1, started.get());
        assertEquals(1, completed.get());
        assertTrue(completedSuccess.get());
    }

    @Test
    void testTaskLifecycleListenerExceptionIsSilent() throws Exception {
        TaskLifecycleListener throwingListener = new TaskLifecycleListener() {
            @Override
            public void onSubmitted(String groupKey, String taskId) {
                throw new RuntimeException("listener error");
            }

            @Override
            public void onStarted(String groupKey, String taskId) {
                throw new RuntimeException("listener error");
            }

            @Override
            public void onCompleted(String groupKey, String taskId, GroupResult<?> result) {
                throw new RuntimeException("listener error");
            }
        };

        GroupPolicy policy = GroupPolicy.builder()
                .taskLifecycleListener(throwingListener)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            // Should not throw despite listener exceptions
            TaskHandle<String> handle = executor.submit("g", "t-1", () -> "ok");
            GroupResult<String> result = handle.await();
            assertEquals(TaskStatus.SUCCESS, result.status());
            assertEquals("ok", result.value());
        }
    }

    @Test
    void testGracefulShutdownWithTimeout() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(2)
                .build();

        GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy);

        // Submit a fast task
        TaskHandle<String> handle = executor.submit("g", "t-1", () -> "done");
        handle.await();

        // Shutdown with generous timeout — should return true
        boolean completed = executor.shutdown(java.time.Duration.ofSeconds(5));
        assertTrue(completed, "should complete within timeout");
    }
}
