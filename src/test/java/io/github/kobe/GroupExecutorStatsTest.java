package io.github.kobe;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GroupExecutorStatsTest {

    @Test
    void testPerGroupStatsWithBlockedTasks() throws Exception {
        // maxConcurrency=2, queue=5: submit 4 tasks that block.
        // 2 should be active, 2 should be queued.
        GroupPolicy policy = GroupPolicy.builder()
                .perGroupMaxConcurrency(Map.of("g", 2))
                .defaultQueueThresholdPerGroup(5)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch allStarted = new CountDownLatch(2);

            for (int i = 0; i < 4; i++) {
                executor.submit("g", "t-" + i, () -> {
                    allStarted.countDown();
                    blockLatch.await();
                    return null;
                });
            }

            // Wait for 2 tasks to start executing
            assertTrue(allStarted.await(5, TimeUnit.SECONDS));
            // Small delay to let queue settle
            Thread.sleep(50);

            Optional<GroupStats> stats = executor.groupStats("g");
            assertTrue(stats.isPresent());
            assertEquals("g", stats.get().groupKey());
            assertEquals(2, stats.get().activeCount());
            assertEquals(2, stats.get().queuedCount());
            assertEquals(2, stats.get().maxConcurrency());
            assertEquals(5, stats.get().queueCapacity());

            blockLatch.countDown();
        }
    }

    @Test
    void testExecutorLevelStatsWithMultipleGroups() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .defaultQueueThresholdPerGroup(5)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch bothStarted = new CountDownLatch(2);

            // Each group: 1 active + 1 queued = 2 tasks
            for (String group : List.of("A", "B")) {
                for (int i = 0; i < 2; i++) {
                    executor.submit(group, group + "-" + i, () -> {
                        bothStarted.countDown();
                        blockLatch.await();
                        return null;
                    });
                }
            }

            assertTrue(bothStarted.await(5, TimeUnit.SECONDS));
            Thread.sleep(50);

            GroupExecutorStats stats = executor.stats();
            assertEquals(2, stats.activeGroupCount());
            assertEquals(2, stats.totalActiveCount());   // 1 per group
            assertEquals(2, stats.totalQueuedCount());    // 1 per group
            assertEquals(Integer.MAX_VALUE, stats.globalInFlightMax());
            assertEquals(0, stats.globalInFlightUsed());  // no global limit

            blockLatch.countDown();
        }
    }

    @Test
    void testGlobalInFlightUsedCorrectness() throws Exception {
        int globalMax = 10;
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(5)
                .globalMaxInFlight(globalMax)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch tasksStarted = new CountDownLatch(3);

            for (int i = 0; i < 3; i++) {
                executor.submit("g", "t-" + i, () -> {
                    tasksStarted.countDown();
                    blockLatch.await();
                    return null;
                });
            }

            assertTrue(tasksStarted.await(5, TimeUnit.SECONDS));

            GroupExecutorStats stats = executor.stats();
            assertEquals(globalMax, stats.globalInFlightMax());
            assertEquals(3, stats.globalInFlightUsed());

            blockLatch.countDown();
        }
    }

    @Test
    void testStatsAfterShutdownReturnsZeroed() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(2)
                .globalMaxInFlight(10)
                .build();

        GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy);

        // Submit a task and wait for completion
        TaskHandle<String> handle = executor.submit("g", "t-1", () -> "done");
        handle.await();

        executor.shutdown();

        // After shutdown: all stats should be zeroed/empty
        GroupExecutorStats stats = executor.stats();
        assertEquals(0, stats.activeGroupCount());
        assertEquals(0, stats.totalActiveCount());
        assertEquals(0, stats.totalQueuedCount());
        assertEquals(0, stats.globalInFlightUsed());
        assertEquals(10, stats.globalInFlightMax());

        Optional<GroupStats> groupStats = executor.groupStats("g");
        assertTrue(groupStats.isEmpty());

        Set<String> keys = executor.activeGroupKeys();
        assertTrue(keys.isEmpty());
    }

    @Test
    void testGroupStatsForNonExistentGroupReturnsEmpty() {
        GroupPolicy policy = GroupPolicy.builder().build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            Optional<GroupStats> stats = executor.groupStats("does-not-exist");
            assertTrue(stats.isEmpty());
        }
    }

    @Test
    void testActiveGroupKeysReflectsSubmissions() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            CountDownLatch blockLatch = new CountDownLatch(1);
            CountDownLatch started = new CountDownLatch(1);

            executor.submit("alpha", "t-1", () -> {
                started.countDown();
                blockLatch.await();
                return null;
            });

            assertTrue(started.await(5, TimeUnit.SECONDS));

            executor.submit("beta", "t-2", () -> {
                blockLatch.await();
                return null;
            });

            Set<String> keys = executor.activeGroupKeys();
            assertTrue(keys.contains("alpha"));
            assertTrue(keys.contains("beta"));
            assertEquals(2, keys.size());

            blockLatch.countDown();
        }
    }

    @Test
    void testActiveGroupKeysClearsAfterEviction() throws Exception {
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(1)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            // Submit and complete a task
            TaskHandle<String> h = executor.submit("evict-me", "t-1", () -> "done");
            h.await();

            assertTrue(executor.activeGroupKeys().contains("evict-me"));

            executor.evictGroup("evict-me");

            assertTrue(!executor.activeGroupKeys().contains("evict-me"),
                    "evicted group should not appear in activeGroupKeys");
        }
    }

    @Test
    void testActiveGroupKeysClearsAfterShutdown() throws Exception {
        GroupPolicy policy = GroupPolicy.builder().build();

        GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy);
        TaskHandle<String> h = executor.submit("g", "t-1", () -> "done");
        h.await();

        executor.shutdown();

        Set<String> keys = executor.activeGroupKeys();
        assertTrue(keys.isEmpty());
    }

    @Test
    void testGroupStatsNullKeyThrowsNPE() {
        GroupPolicy policy = GroupPolicy.builder().build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            assertThrows(NullPointerException.class, () -> executor.groupStats(null));
        }
    }

    @Test
    void testStatsConcurrentWithSubmit() throws Exception {
        // Call stats() concurrently with submit() — no exceptions should be thrown.
        GroupPolicy policy = GroupPolicy.builder()
                .defaultMaxConcurrencyPerGroup(5)
                .build();

        try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
            int iterations = 200;
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(iterations * 2);
            AtomicReference<Throwable> error = new AtomicReference<>();

            // Submit tasks in parallel
            for (int i = 0; i < iterations; i++) {
                final int idx = i;
                Thread.ofVirtual().start(() -> {
                    try {
                        start.await();
                        executor.submit("g", "t-" + idx, () -> {
                            Thread.sleep(1);
                            return null;
                        });
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    } finally {
                        done.countDown();
                    }
                });
            }

            // Call stats() in parallel
            for (int i = 0; i < iterations; i++) {
                Thread.ofVirtual().start(() -> {
                    try {
                        start.await();
                        assertDoesNotThrow(() -> {
                            executor.stats();
                            executor.groupStats("g");
                            executor.activeGroupKeys();
                        });
                    } catch (Throwable t) {
                        error.compareAndSet(null, t);
                    } finally {
                        done.countDown();
                    }
                });
            }

            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS), "all threads should finish");
            if (error.get() != null) {
                throw new AssertionError("Concurrent access caused an exception", error.get());
            }
        }
    }
}
