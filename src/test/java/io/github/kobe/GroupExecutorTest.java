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
import java.util.concurrent.atomic.AtomicReference;

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
    void testGlobalInFlightLimit() throws Exception {
        // Layer 1: global in-flight limit ensures total tasks across all groups is bounded
        int globalLimit = 4;
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(10)
                .globalMaxInFlight(globalLimit)
                .rejectionPolicy(RejectionPolicy.DISCARD)
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
            done.await(10, TimeUnit.SECONDS);
            for (TaskHandle<Void> handle : handles) {
                // Some tasks may be REJECTED because globalMaxInFlight is 4 and we have 12 tasks
                GroupResult<Void> result = handle.await();
                assertTrue(result.status() == TaskStatus.SUCCESS || result.status() == TaskStatus.REJECTED,
                        "Expected SUCCESS or REJECTED, got " + result.status());
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

            // Shutdown group B (which doesn't exist) — should not affect group A
            executor.shutdownGroup("B");

            // Group A task should still complete
            groupABlock.countDown();
            GroupResult<String> resultA = handleA.await();
            assertEquals(TaskStatus.SUCCESS, resultA.status());
            assertEquals("A-done", resultA.value());

            // Should be able to submit new tasks to group B (state recreated)
            TaskHandle<String> handleB = executor.submit("B", "b-1", () -> "B-done");
            GroupResult<String> resultB = handleB.await();
            assertEquals(TaskStatus.SUCCESS, resultB.status());
            assertEquals("B-done", resultB.value());
        }
    }

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
    void testCancelInterruptsRunningTask() throws Exception {
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
    void testBuilderRejectsInvalidValues() {
        assertThrows(IllegalArgumentException.class, () ->
                GroupPolicy.builder().defaultMaxConcurrencyPerGroup(0).build());
        assertThrows(IllegalArgumentException.class, () ->
                GroupPolicy.builder().defaultMaxConcurrencyPerGroup(-1).build());
        assertThrows(IllegalArgumentException.class, () ->
                GroupPolicy.builder().globalMaxInFlight(0).build());
        assertThrows(IllegalArgumentException.class, () ->
                GroupPolicy.builder().defaultQueueThresholdPerGroup(-1).build());
        assertThrows(IllegalArgumentException.class, () ->
                GroupPolicy.builder().perGroupMaxConcurrency(Map.of("g", 0)).build());
        assertThrows(IllegalArgumentException.class, () ->
                GroupPolicy.builder().perGroupQueueThreshold(Map.of("g", -1)).build());
    }

    @Test
    void testQueueCapacityRejectsWhenFull() throws Exception {
        // maxConcurrency=1, queueCapacity=1: 1 running + 1 queued = 2 tasks max
        // The 3rd task should be rejected.
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(1)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            // First task: running, holds the concurrency permit
            TaskHandle<String> h1 = executor.submit("g", "t-1", () -> {
                startedLatch.countDown();
                blockLatch.await();
                return "first";
            });

            startedLatch.await(5, TimeUnit.SECONDS);

            // Second task: queued (queue capacity = 1)
            TaskHandle<String> h2 = executor.submit("g", "t-2", () -> "second");

            // Third task: queue is full → REJECTED
            TaskHandle<String> h3 = executor.submit("g", "t-3", () -> "third");

            // Unblock task 1
            blockLatch.countDown();

            GroupResult<String> r1 = h1.await();
            GroupResult<String> r2 = h2.await();
            GroupResult<String> r3 = h3.await();

            assertEquals(TaskStatus.SUCCESS, r1.status());
            assertEquals("first", r1.value());
            assertEquals(TaskStatus.SUCCESS, r2.status());
            assertEquals("second", r2.value());
            assertEquals(TaskStatus.REJECTED, r3.status());
        }
    }

    @Test
    void testAutoDispatchAfterTaskCompletion() throws Exception {
        // Verify that a queued task is automatically dispatched when a running task completes.
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch firstBlock = new CountDownLatch(1);
            CountDownLatch secondStarted = new CountDownLatch(1);

            TaskHandle<String> h1 = executor.submit("g", "t-1", () -> {
                firstStarted.countDown();
                firstBlock.await();
                return "first";
            });

            firstStarted.await(5, TimeUnit.SECONDS);

            // This task gets queued since concurrency=1 and t-1 is running
            TaskHandle<String> h2 = executor.submit("g", "t-2", () -> {
                secondStarted.countDown();
                return "second";
            });

            // Unblock t-1 — t-2 should be auto-dispatched
            firstBlock.countDown();

            // t-2 should start automatically after t-1 completes
            assertTrue(secondStarted.await(5, TimeUnit.SECONDS), "second task should start after first completes");

            assertEquals(TaskStatus.SUCCESS, h1.await().status());
            assertEquals(TaskStatus.SUCCESS, h2.await().status());
        }
    }

    @Test
    void testCallerRunsExecutesInSubmitThread() throws Exception {
        // CALLER_RUNS should execute in the thread that called submit()
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(0)
                .rejectionPolicy(RejectionPolicy.CALLER_RUNS)
                .build();

        AtomicReference<Thread> taskThread = new AtomicReference<>();
        Thread submitThread = Thread.currentThread();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            // First task holds the concurrency permit
            executor.submit("g", "t-1", () -> {
                startedLatch.countDown();
                blockLatch.await();
                return "first";
            });

            startedLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(50); // ensure t-1 is running

            // Second task: concurrency permit unavailable, queue=0 → CALLER_RUNS
            TaskHandle<String> h2 = executor.submit("g", "t-2", () -> {
                taskThread.set(Thread.currentThread());
                return "caller-ran";
            });

            // h2 should already be complete because it ran in the submit thread
            GroupResult<String> r2 = h2.await();
            assertEquals(TaskStatus.SUCCESS, r2.status());
            assertEquals("caller-ran", r2.value());
            // CALLER_RUNS executes in the submit() calling thread (this test thread)
            assertEquals(submitThread, taskThread.get(),
                    "CALLER_RUNS should execute in the thread that called submit()");

            blockLatch.countDown();
        }
    }

    @Test
    void testCancelQueuedTask() throws Exception {
        // A task that is queued (not yet running) and then cancelled should get CANCELLED result
        // and the next queued task should be dispatched correctly.
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch firstStarted = new CountDownLatch(1);

            // t-1: running, blocking
            TaskHandle<String> h1 = executor.submit("g", "t-1", () -> {
                firstStarted.countDown();
                blockLatch.await();
                return "first";
            });

            firstStarted.await(5, TimeUnit.SECONDS);

            // t-2: queued (will be cancelled before it runs)
            TaskHandle<String> h2 = executor.submit("g", "t-2", () -> "second");

            // t-3: also queued
            TaskHandle<String> h3 = executor.submit("g", "t-3", () -> "third");

            // Cancel t-2 while it's queued
            h2.cancel(true);

            // Unblock t-1
            blockLatch.countDown();

            GroupResult<String> r1 = h1.await();
            // t-2 future was cancelled
            GroupResult<String> r2 = h2.await();
            GroupResult<String> r3 = h3.await();

            assertEquals(TaskStatus.SUCCESS, r1.status());
            assertEquals(TaskStatus.CANCELLED, r2.status());
            assertEquals(TaskStatus.SUCCESS, r3.status());
            assertEquals("third", r3.value());
        }
    }

    @Test
    void testGracefulShutdownDrainsQueueAsRejected() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .build();

        GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy);

        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch firstStarted = new CountDownLatch(1);

        // t-1: running and blocking
        TaskHandle<String> h1 = executor.submit("g", "t-1", () -> {
            firstStarted.countDown();
            blockLatch.await();
            return "first";
        });

        firstStarted.await(5, TimeUnit.SECONDS);

        // t-2: queued
        TaskHandle<String> h2 = executor.submit("g", "t-2", () -> "second");

        // Unblock t-1 before shutdown
        blockLatch.countDown();
        h1.await();

        // Now shutdown — any still-queued tasks should get REJECTED
        executor.shutdown();

        GroupResult<String> r2 = h2.join();
        // t-2 was either dispatched before shutdown or rejected by shutdown drain
        assertTrue(r2.status() == TaskStatus.SUCCESS || r2.status() == TaskStatus.REJECTED,
                "Queued task should be SUCCESS or REJECTED after shutdown, got: " + r2.status());
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

    @Test
    void testGlobalPermitReclaimedAcrossMultipleRounds() throws Exception {
        // Verifies that global permits are fully reclaimed after tasks complete via hand-off.
        // With globalMaxInFlight=2 and concurrency=1 per group, tasks queue and trigger
        // hand-off in onTaskDone. After each round completes, permits must be fully reclaimed
        // so the next round can submit successfully.
        int globalLimit = 2;
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .globalMaxInFlight(globalLimit)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            // Run 5 rounds. Each round submits 4 tasks (2 groups × 2 tasks per group).
            // With globalMaxInFlight=2, each round will have 2 accepted + 2 rejected.
            // If global permits leak, later rounds will reject ALL tasks.
            for (int round = 0; round < 5; round++) {
                List<TaskHandle<String>> handles = new ArrayList<>();
                for (int g = 0; g < 2; g++) {
                    for (int t = 0; t < 2; t++) {
                        String taskId = "r" + round + "-g" + g + "-t" + t;
                        handles.add(executor.submit("group-" + g, taskId, () -> {
                            Thread.sleep(10);
                            return "done";
                        }));
                    }
                }

                int successCount = 0;
                for (TaskHandle<String> h : handles) {
                    GroupResult<String> r = h.join();
                    if (r.status() == TaskStatus.SUCCESS) successCount++;
                }

                assertTrue(successCount >= 2,
                        "round " + round + ": expected at least 2 successes but got " + successCount
                                + " — global permits may be leaking");
            }
        }
    }

    @Test
    void testGlobalPermitFullReclamationAfterHandOff() throws Exception {
        // Targeted test: concurrency=1, queue=5, globalMaxInFlight=3
        // Submit 3 tasks to same group: 1 runs, 2 queued. All 3 global permits consumed.
        // After all 3 complete (two via hand-off), all 3 global permits must be reclaimed.
        // Then submit 3 more — all must succeed.
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(5)
                .globalMaxInFlight(3)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            // Round 1: 3 tasks → 1 runs + 2 queued (hand-off chain)
            List<TaskHandle<String>> round1 = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                round1.add(executor.submit("g", "r1-t" + i, () -> {
                    Thread.sleep(10);
                    return "ok";
                }));
            }
            for (TaskHandle<String> h : round1) {
                assertEquals(TaskStatus.SUCCESS, h.join().status(), "round 1 task should succeed");
            }

            // Round 2: if permits leaked, these would be rejected
            List<TaskHandle<String>> round2 = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                round2.add(executor.submit("g", "r2-t" + i, () -> {
                    Thread.sleep(10);
                    return "ok";
                }));
            }
            for (TaskHandle<String> h : round2) {
                assertEquals(TaskStatus.SUCCESS, h.join().status(),
                        "round 2 task should succeed — all global permits must have been reclaimed");
            }
        }
    }

    @Test
    void testQueuedTasksAllCompleteAfterRunningTaskFinishes() throws Exception {
        // Submit many tasks with concurrency=2, queue=8.
        // All tasks should eventually complete.
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(2)
                .build();

        int total = 10;
        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch allDone = new CountDownLatch(total);
            List<TaskHandle<Integer>> handles = new ArrayList<>();

            for (int i = 0; i < total; i++) {
                final int idx = i;
                handles.add(executor.submit("g", "t-" + i, () -> {
                    Thread.sleep(10);
                    allDone.countDown();
                    return idx;
                }));
            }

            assertTrue(allDone.await(30, TimeUnit.SECONDS), "all tasks should complete");
            for (int i = 0; i < total; i++) {
                GroupResult<Integer> r = handles.get(i).await();
                assertEquals(TaskStatus.SUCCESS, r.status());
                assertEquals(i, r.value());
            }
        }
    }
}
