package io.github.kobe;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
