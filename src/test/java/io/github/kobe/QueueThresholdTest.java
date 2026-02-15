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
