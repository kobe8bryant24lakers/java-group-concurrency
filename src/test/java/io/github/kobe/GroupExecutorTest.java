package io.github.kobe;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
}
