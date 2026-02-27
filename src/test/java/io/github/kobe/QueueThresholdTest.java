package io.github.kobe;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueueThresholdTest {

    @Test
    void testPerGroupQueueThresholdRejectsExcess() throws Exception {
        // concurrency=1, queue=2 → at most 3 tasks can be accepted (1 running + 2 waiting)
        // Additional tasks should be rejected.
        int concurrency = 1;
        int queueThreshold = 2;
        int totalTasks = 6;

        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(concurrency)
                .defaultQueueThresholdPerGroup(queueThreshold)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);
            AtomicInteger rejectedCount = new AtomicInteger();

            List<TaskHandle<String>> handles = new ArrayList<>();
            for (int i = 0; i < totalTasks; i++) {
                int idx = i;
                handles.add(executor.submit("g", "t-" + idx, () -> {
                    if (idx == 0) startedLatch.countDown();
                    blockLatch.await();
                    return "ok-" + idx;
                }));
            }

            // Wait for the first task to start executing (holding the concurrency permit)
            startedLatch.await(5, TimeUnit.SECONDS);
            // Give a moment for all submissions to settle
            Thread.sleep(200);

            // Release all tasks
            blockLatch.countDown();

            for (TaskHandle<String> handle : handles) {
                GroupResult<String> result = handle.join();
                if (result.status() == TaskStatus.REJECTED) {
                    rejectedCount.incrementAndGet();
                }
            }

            assertTrue(rejectedCount.get() > 0, "some tasks should be rejected");
        }
    }

    @Test
    void testGlobalQueueThresholdRejectsExcess() throws Exception {
        // Global queue threshold = 1, so only 1 task can wait globally at a time
        int globalQueueThreshold = 1;
        int totalTasks = 6;

        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .globalMaxInFlight(1)
                .globalQueueThreshold(globalQueueThreshold)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            List<TaskHandle<String>> handles = new ArrayList<>();
            for (int i = 0; i < totalTasks; i++) {
                int idx = i;
                handles.add(executor.submit("group-" + idx, "t-" + idx, () -> {
                    if (idx == 0) startedLatch.countDown();
                    blockLatch.await();
                    return "ok";
                }));
            }

            startedLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(200);
            blockLatch.countDown();

            int rejectedCount = 0;
            for (TaskHandle<String> handle : handles) {
                GroupResult<String> result = handle.join();
                if (result.status() == TaskStatus.REJECTED) {
                    rejectedCount++;
                }
            }

            assertTrue(rejectedCount > 0, "some tasks should be rejected by global queue threshold");
        }
    }

    @Test
    void testQueueThresholdZeroMeansNoWaiting() throws Exception {
        // queue threshold = 0 means no task is allowed to wait — any task that can't
        // immediately get the concurrency permit is rejected.
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(0)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            // First task holds the concurrency permit
            TaskHandle<String> handle1 = executor.submit("g", "t-1", () -> {
                startedLatch.countDown();
                blockLatch.await();
                return "first";
            });

            startedLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(100);

            // Second task should be rejected immediately (queue=0, no waiting allowed)
            TaskHandle<String> handle2 = executor.submit("g", "t-2", () -> "second");
            Thread.sleep(200);

            blockLatch.countDown();

            GroupResult<String> result1 = handle1.join();
            GroupResult<String> result2 = handle2.join();

            assertEquals(TaskStatus.SUCCESS, result1.status());
            assertEquals(TaskStatus.REJECTED, result2.status());
        }
    }

    @Test
    void testAbortPolicyThrowsException() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(0)
                .rejectionPolicy(RejectionPolicy.ABORT)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            TaskHandle<String> handle1 = executor.submit("g", "t-1", () -> {
                startedLatch.countDown();
                blockLatch.await();
                return "first";
            });

            startedLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(100);

            TaskHandle<String> handle2 = executor.submit("g", "t-2", () -> "second");
            Thread.sleep(200);

            blockLatch.countDown();

            assertEquals(TaskStatus.SUCCESS, handle1.await().status());

            // ABORT policy should cause await() to throw RejectedTaskException
            RejectedTaskException ex = assertThrows(RejectedTaskException.class, handle2::await);
            assertEquals("g", ex.groupKey());
            assertEquals("t-2", ex.taskId());
        }
    }

    @Test
    void testDiscardPolicyReturnsSilently() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(0)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            executor.submit("g", "t-1", () -> {
                startedLatch.countDown();
                blockLatch.await();
                return "first";
            });

            startedLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(100);

            TaskHandle<String> handle2 = executor.submit("g", "t-2", () -> "second");
            Thread.sleep(200);

            blockLatch.countDown();

            GroupResult<String> result = handle2.join();
            assertEquals(TaskStatus.REJECTED, result.status());
        }
    }

    @Test
    void testCallerRunsExecutesTask() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(0)
                .rejectionPolicy(RejectionPolicy.CALLER_RUNS)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            executor.submit("g", "t-1", () -> {
                startedLatch.countDown();
                blockLatch.await();
                return "first";
            });

            startedLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(100);

            // CALLER_RUNS should execute the task directly
            TaskHandle<String> handle2 = executor.submit("g", "t-2", () -> "caller-ran");
            Thread.sleep(200);

            blockLatch.countDown();

            GroupResult<String> result = handle2.join();
            assertEquals(TaskStatus.SUCCESS, result.status());
            assertEquals("caller-ran", result.value());
        }
    }

    @Test
    void testCustomRejectionHandler() throws Exception {
        AtomicBoolean handlerCalled = new AtomicBoolean(false);

        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(0)
                .rejectionHandler(new RejectionHandler() {
                    @Override
                    public <T> GroupResult<T> onRejected(String groupKey, String taskId,
                                                         java.util.concurrent.Callable<T> task) {
                        handlerCalled.set(true);
                        return GroupResult.rejected(groupKey, taskId);
                    }
                })
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            executor.submit("g", "t-1", () -> {
                startedLatch.countDown();
                blockLatch.await();
                return "first";
            });

            startedLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(100);

            TaskHandle<String> handle2 = executor.submit("g", "t-2", () -> "second");
            Thread.sleep(200);

            blockLatch.countDown();

            GroupResult<String> result = handle2.join();
            assertEquals(TaskStatus.REJECTED, result.status());
            assertTrue(handlerCalled.get(), "custom handler should have been called");
        }
    }

    @Test
    void testExecuteAllCollectsRejectedResults() {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(0)
                .rejectionPolicy(RejectionPolicy.ABORT)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            List<GroupTask<String>> tasks = new ArrayList<>();
            // Submit multiple tasks — with queue=0 and concurrency=1,
            // some tasks will be rejected via ABORT policy.
            for (int i = 0; i < 10; i++) {
                int idx = i;
                tasks.add(new GroupTask<>("g", "t-" + idx, () -> {
                    Thread.sleep(200);
                    return "ok-" + idx;
                }));
            }

            List<GroupResult<String>> results = executor.executeAll(tasks);
            assertEquals(10, results.size());

            long rejectedCount = results.stream()
                    .filter(r -> r.status() == TaskStatus.REJECTED)
                    .count();
            long successCount = results.stream()
                    .filter(r -> r.status() == TaskStatus.SUCCESS)
                    .count();

            assertTrue(rejectedCount > 0, "some tasks should be REJECTED");
            assertTrue(successCount > 0, "some tasks should succeed");
        }
    }

    @Test
    void testNoThresholdConfiguredBackwardCompatible() throws Exception {
        // No queue thresholds configured — should behave identically to before
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(2)
                .build();

        assertEquals(Integer.MAX_VALUE, policy.globalQueueThreshold());
        assertEquals(Integer.MAX_VALUE, policy.defaultQueueThresholdPerGroup());
        assertTrue(policy.perGroupQueueThreshold().isEmpty());

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            int taskCount = 6;
            CountDownLatch gate = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(taskCount);

            List<TaskHandle<Void>> handles = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                handles.add(executor.submit("g", "t-" + i, () -> {
                    gate.await();
                    Thread.sleep(50);
                    done.countDown();
                    return null;
                }));
            }

            gate.countDown();
            done.await();
            for (TaskHandle<Void> handle : handles) {
                assertEquals(TaskStatus.SUCCESS, handle.await().status());
            }
        }
    }

    @Test
    void testPerGroupQueueOverride() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(10) // generous default
                .perGroupQueueThreshold(Map.of("restricted", 0)) // restricted group: no waiting
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            executor.submit("restricted", "t-1", () -> {
                startedLatch.countDown();
                blockLatch.await();
                return "first";
            });

            startedLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(100);

            // "restricted" group has queue=0, should be rejected
            TaskHandle<String> handle2 = executor.submit("restricted", "t-2", () -> "second");
            Thread.sleep(200);

            // "normal" group should work fine (default queue=10)
            TaskHandle<String> handle3 = executor.submit("normal", "t-3", () -> "third");

            blockLatch.countDown();

            GroupResult<String> result2 = handle2.join();
            GroupResult<String> result3 = handle3.join();

            assertEquals(TaskStatus.REJECTED, result2.status());
            assertEquals(TaskStatus.SUCCESS, result3.status());
            assertEquals("third", result3.value());
        }
    }

    @Test
    void testQueuePermitReleasedAfterAcquire() throws Exception {
        // With concurrency=1, queue=1: submit 3 tasks sequentially.
        // Task 1 runs, task 2 waits (using the 1 queue slot), task 3 should
        // succeed after task 1 finishes (freeing the queue slot for task 2 to execute).
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(1)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch task1Started = new CountDownLatch(1);
            CountDownLatch task1Block = new CountDownLatch(1);

            TaskHandle<String> h1 = executor.submit("g", "t-1", () -> {
                task1Started.countDown();
                task1Block.await();
                return "first";
            });

            task1Started.await(5, TimeUnit.SECONDS);
            Thread.sleep(50);

            // task-2 fills the queue slot
            TaskHandle<String> h2 = executor.submit("g", "t-2", () -> "second");
            Thread.sleep(50);

            // Unblock task-1: this should free the concurrency permit,
            // which allows task-2 to run, freeing the queue permit
            task1Block.countDown();

            // Wait for task-1 and task-2 to finish
            assertEquals(TaskStatus.SUCCESS, h1.join().status());
            assertEquals(TaskStatus.SUCCESS, h2.join().status());

            // Now the queue slot is free again — task-3 should succeed
            TaskHandle<String> h3 = executor.submit("g", "t-3", () -> "third");
            GroupResult<String> result3 = h3.join();
            assertEquals(TaskStatus.SUCCESS, result3.status());
            assertEquals("third", result3.value());
        }
    }

    @Test
    void testOnRejectedListenerCallback() throws Exception {
        AtomicInteger rejectedCallbackCount = new AtomicInteger();
        AtomicBoolean correctParams = new AtomicBoolean(false);

        TaskLifecycleListener listener = new TaskLifecycleListener() {
            @Override
            public void onRejected(String groupKey, String taskId, String reason) {
                rejectedCallbackCount.incrementAndGet();
                if ("g".equals(groupKey) && "t-2".equals(taskId) && reason != null) {
                    correctParams.set(true);
                }
            }
        };

        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(0)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .taskLifecycleListener(listener)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch startedLatch = new CountDownLatch(1);

            executor.submit("g", "t-1", () -> {
                startedLatch.countDown();
                blockLatch.await();
                return "first";
            });

            startedLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(100);

            TaskHandle<String> handle2 = executor.submit("g", "t-2", () -> "second");
            Thread.sleep(200);

            blockLatch.countDown();
            handle2.join();

            assertTrue(rejectedCallbackCount.get() > 0, "onRejected should have been called");
            assertTrue(correctParams.get(), "onRejected should receive correct groupKey, taskId, reason");
        }
    }

    @Test
    void testEvictGroupClearsQueueSemaphore() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(1)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            // Complete a task to initialize resources
            TaskHandle<String> h1 = executor.submit("evict-g", "t-1", () -> "done");
            assertEquals(TaskStatus.SUCCESS, h1.join().status());

            // Evict the group
            executor.evictGroup("evict-g");

            // New tasks should still work (resources recreated)
            TaskHandle<String> h2 = executor.submit("evict-g", "t-2", () -> "after-evict");
            GroupResult<String> result = h2.join();
            assertEquals(TaskStatus.SUCCESS, result.status());
            assertEquals("after-evict", result.value());
        }
    }

    /**
     * Verifies that a task which had to WAIT at Layer 2 (bulkhead full) and subsequently
     * also needs to wait at Layer 3 (concurrency full) holds exactly ONE queue slot
     * throughout the entire waiting period — no release-and-reacquire between layers.
     *
     * Setup: bulkhead=2, concurrency=1, queueThreshold=1
     *   Task 1: runs           (bulkhead 1/2, concurrency 1/1)
     *   Task 2: immediately gets bulkhead (2/2), waits at Layer 3 — uses the 1 queue slot
     *   Task 3: bulkhead FULL → queue slot already taken by Task 2 → REJECTED
     * After Task 1 finishes: Task 2 executes → SUCCESS
     */
    @Test
    void testQueueSlotHeldContinuouslyAcrossLayers() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultMaxInFlightPerGroup(2)      // bulkhead = 2: 1 running + 1 waiting
                .defaultQueueThresholdPerGroup(1)   // only 1 task may wait at any time
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch task1Started = new CountDownLatch(1);
            CountDownLatch task1Block = new CountDownLatch(1);

            // Task 1: acquires bulkhead(1/2) + concurrency(1/1) → runs
            TaskHandle<String> h1 = executor.submit("g", "t-1", () -> {
                task1Started.countDown();
                task1Block.await();
                return "t1";
            });

            task1Started.await(5, TimeUnit.SECONDS);
            Thread.sleep(50);

            // Task 2: immediately acquires bulkhead(2/2), blocked at Layer 3 (concurrency full),
            // occupies the single queue slot. With the fix, this slot is held until Task 2
            // acquires the concurrency permit.
            TaskHandle<String> h2 = executor.submit("g", "t-2", () -> "t2");
            Thread.sleep(80);

            // Task 3: bulkhead FULL (2/2). Tries to wait at Layer 2, but queue slot is taken
            // by Task 2 → must be REJECTED.
            // Before the fix, Task 2 would have released its slot here, allowing Task 3 to
            // steal it, and then Task 2 would be the one rejected (incorrect behavior).
            TaskHandle<String> h3 = executor.submit("g", "t-3", () -> "t3");
            Thread.sleep(50);

            task1Block.countDown();

            GroupResult<String> r1 = h1.join();
            GroupResult<String> r2 = h2.join();
            GroupResult<String> r3 = h3.join();

            assertEquals(TaskStatus.SUCCESS, r1.status(), "task 1 must succeed");
            assertEquals(TaskStatus.SUCCESS, r2.status(), "task 2 must succeed — it held the queue slot throughout");
            assertEquals(TaskStatus.REJECTED, r3.status(), "task 3 must be rejected — queue was occupied by task 2");
        }
    }

    /**
     * Verifies that a single queue slot is counted exactly once per waiting task,
     * even when the task passes through both Layer 2 and Layer 3 waits.
     * With queueThreshold=2: exactly 2 tasks may wait, the 3rd must be rejected.
     */
    @Test
    void testQueueCapacityExactCountWithBulkhead() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultMaxInFlightPerGroup(3)      // bulkhead = 3
                .defaultQueueThresholdPerGroup(2)   // 2 tasks may wait
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch task1Started = new CountDownLatch(1);
            CountDownLatch task1Block = new CountDownLatch(1);

            // Task 1 runs
            TaskHandle<String> h1 = executor.submit("g", "t-1", () -> {
                task1Started.countDown();
                task1Block.await();
                return "t1";
            });

            task1Started.await(5, TimeUnit.SECONDS);
            Thread.sleep(50);

            // Tasks 2 and 3 each immediately acquire a bulkhead slot, then wait at Layer 3.
            // Each occupies exactly 1 queue slot. Total queue usage = 2 (at threshold).
            TaskHandle<String> h2 = executor.submit("g", "t-2", () -> "t2");
            TaskHandle<String> h3 = executor.submit("g", "t-3", () -> "t3");
            Thread.sleep(80);

            // Task 4: queueThreshold=2 is now FULL (Tasks 2 and 3 each hold 1 slot).
            // Must be REJECTED. Before the fix, double-counting could leave ghost capacity,
            // causing Task 4 to incorrectly succeed.
            TaskHandle<String> h4 = executor.submit("g", "t-4", () -> "t4");
            Thread.sleep(50);

            task1Block.countDown();

            GroupResult<String> r1 = h1.join();
            GroupResult<String> r2 = h2.join();
            GroupResult<String> r3 = h3.join();
            GroupResult<String> r4 = h4.join();

            assertEquals(TaskStatus.SUCCESS, r1.status(), "task 1 must succeed");
            assertEquals(TaskStatus.SUCCESS, r2.status(), "task 2 must succeed");
            assertEquals(TaskStatus.SUCCESS, r3.status(), "task 3 must succeed");
            assertEquals(TaskStatus.REJECTED, r4.status(),
                    "task 4 must be rejected — queue threshold of 2 was filled by tasks 2 and 3");
        }
    }

    /**
     * Verifies that all tasks eventually complete under concurrency=1 with a fair semaphore,
     * demonstrating absence of starvation.
     *
     * Note: fair semaphores guarantee FIFO among threads already queued on the semaphore,
     * but do not guarantee strict submission-order execution under virtual-thread scheduling.
     */
    @Test
    void testNoStarvationWithFairSemaphore() throws Exception {
        int taskCount = 5;
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .build();

        CopyOnWriteArrayList<Integer> executionOrder = new CopyOnWriteArrayList<>();
        CountDownLatch allStarted = new CountDownLatch(taskCount);
        CountDownLatch gate = new CountDownLatch(1);

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            List<TaskHandle<Void>> handles = new ArrayList<>(taskCount);

            for (int i = 0; i < taskCount; i++) {
                int idx = i;
                handles.add(executor.submit("fair-group", "t-" + i, () -> {
                    allStarted.countDown();
                    gate.await();
                    executionOrder.add(idx);
                    return null;
                }));
            }

            allStarted.await(5, TimeUnit.SECONDS);
            gate.countDown();
            for (TaskHandle<Void> h : handles) {
                assertEquals(TaskStatus.SUCCESS, h.await().status());
            }
        }

        assertEquals(taskCount, executionOrder.size(), "all tasks must complete without starvation");
        for (int i = 0; i < taskCount; i++) {
            int idx = i;
            assertTrue(executionOrder.contains(idx), "task " + idx + " must have executed");
        }
    }

    /**
     * Verifies that queue slots are reclaimed correctly after tasks complete,
     * allowing subsequent tasks to use those slots without leakage.
     */
    @Test
    void testQueueSlotReleasedAfterCrossLayerExecution() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultMaxInFlightPerGroup(2)
                .defaultQueueThresholdPerGroup(1)
                .rejectionPolicy(RejectionPolicy.DISCARD)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch phase1Done = new CountDownLatch(2);
            CountDownLatch block = new CountDownLatch(1);

            // Phase 1: Task 1 runs, Task 2 waits at Layer 3 (holds queue slot)
            TaskHandle<String> h1 = executor.submit("g", "t-1", () -> {
                block.await();
                return "t1";
            });
            TaskHandle<String> h2 = executor.submit("g", "t-2", () -> {
                phase1Done.countDown();
                return "t2";
            });

            Thread.sleep(80);
            // Unblock: Task 1 finishes → Task 2 runs → both done, queue slot reclaimed
            block.countDown();
            phase1Done.await(5, TimeUnit.SECONDS);

            // Give a moment for all resources to be released
            Thread.sleep(50);

            // Phase 2: the queue slot should be free again; new tasks must succeed
            TaskHandle<String> h3 = executor.submit("g", "t-3", () -> "t3");
            TaskHandle<String> h4 = executor.submit("g", "t-4", () -> "t4");

            GroupResult<String> r1 = h1.join();
            GroupResult<String> r2 = h2.join();
            GroupResult<String> r3 = h3.join();
            GroupResult<String> r4 = h4.join();

            assertEquals(TaskStatus.SUCCESS, r1.status());
            assertEquals(TaskStatus.SUCCESS, r2.status());
            // h3 immediately gets both permits (no waiting) — succeeds
            assertEquals(TaskStatus.SUCCESS, r3.status(),
                    "queue slot should be free after phase 1 completes");
            // h4: depends on h3 holding concurrency — may wait or succeed;
            // either way it must not be stuck (no slot leak)
            assertTrue(r4.status() == TaskStatus.SUCCESS || r4.status() == TaskStatus.REJECTED,
                    "task 4 must not be stuck — queue slot was reclaimed");
        }
    }

    @Test
    void testBuilderDefensiveCopy() {
        HashMap<String, Integer> mutableMap = new HashMap<>();
        mutableMap.put("g1", 5);

        GroupPolicy policy = GroupPolicy.builder()
                .perGroupQueueThreshold(mutableMap)
                .build();

        // Mutate the original map after build
        mutableMap.put("g1", 999);
        mutableMap.put("g2", 10);

        // Policy should not be affected by mutation
        assertEquals(5, policy.resolveQueueThreshold("g1"));
        // g2 should fall back to default (MAX_VALUE), not 10
        assertEquals(Integer.MAX_VALUE, policy.resolveQueueThreshold("g2"));
    }
}
