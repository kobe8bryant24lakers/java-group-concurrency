# group-concurrency

A lightweight Java library for **grouped concurrency control** built on JDK 21 Virtual Threads.

Tasks are partitioned by `groupKey` and groups run in parallel, while three layers of fair semaphores enforce concurrency limits at the global, per-group in-flight, and per-group execution levels.

**Zero runtime dependencies** -- only JDK 21; JUnit 5 is test-scoped.

---

## Quick Start

**Maven**
```xml
<dependency>
  <groupId>io.github.kobe</groupId>
  <artifactId>group-concurrency</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

**Basic usage**
```java
import io.github.kobe.*;
import java.util.List;
import java.util.Map;

GroupPolicy policy = GroupPolicy.builder()
        .perGroupMaxConcurrency(Map.of("groupA", 2))
        .concurrencyResolver(key -> key.startsWith("vip:") ? 4 : 1)
        .defaultMaxConcurrencyPerGroup(1)
        .build();

try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
    List<GroupTask<String>> tasks = List.of(
            new GroupTask<>("vip:alpha", "t1", () -> "hi vip"),
            new GroupTask<>("groupA",    "t2", () -> "hi A"),
            new GroupTask<>("user:plain","t3", () -> "hi user")
    );
    List<GroupResult<String>> results = executor.executeAll(tasks);
    results.forEach(r ->
        System.out.printf("%s/%s -> %s: %s%n",
            r.groupKey(), r.taskId(), r.status(), r.value()));
}
```

---

## Three-Layer Protection Model

Semaphores are acquired in order (1 -> 2 -> 3) and released in reverse to prevent deadlocks. All semaphores use **fair mode** to prevent starvation.

```
              submit("groupA", "t1", task)
                        |
                        v
        +-------------------------------+
        | Global Queue Threshold Check  |  tryAcquire() -- reject if full
        +-------------------------------+
                        |
                        v
        +-------------------------------+
        | Layer 1: Global In-Flight     |
        | caps total tasks across ALL   |
        | groups (globalMaxInFlight)    |
        +-------------------------------+
                  | acquire() done → release global queue permit
                  v
        +-------------------------------+
        | Layer 2: Per-Group Bulkhead   |
        | caps queued + running tasks   |
        | per group (maxInFlightPerGrp) |
        +-------------------------------+
                        |
                        v
        +-------------------------------+
        | Per-Group Queue Threshold     |  tryAcquire() -- reject if full
        +-------------------------------+
                        |
                        v
        +-------------------------------+
        | Layer 3: Per-Group Concurrency|
        | caps actual concurrent        |
        | execution per group           |
        +-------------------------------+
                  | acquire() done → release per-group queue permit
                  v
                 Task executes on
              per-group virtual thread
```

> Queue threshold checks use an optimistic strategy: the protection-layer semaphore is tried non-blocking first (`tryAcquire()`). Only when the permit is not immediately available does the queue threshold check kick in. This means threshold=0 correctly allows tasks that don't need to wait.

| Component | Scope | Default | Purpose |
|-----------|-------|---------|---------|
| Global Queue | Global | `Integer.MAX_VALUE` (off) | Limit tasks waiting for Layer 1 |
| Layer 1 | Global | `Integer.MAX_VALUE` | Prevent system-wide overload |
| Layer 2 | Per-group | `Integer.MAX_VALUE` | Limit queued + running tasks per group |
| Per-Group Queue | Per-group | `Integer.MAX_VALUE` (off) | Limit tasks waiting for Layer 3 |
| Layer 3 | Per-group | `1` | Limit actual concurrency per group |

---

## Core API

### GroupPolicy

Configure concurrency limits via the builder:

```java
GroupPolicy policy = GroupPolicy.builder()
    // --- Layer 3: Per-group concurrency (resolution priority: high -> low) ---
    .perGroupMaxConcurrency(Map.of("groupA", 2, "groupB", 5))  // 1st: explicit overrides
    .concurrencyResolver(key -> key.startsWith("vip:") ? 4 : 1) // 2nd: dynamic resolver
    .defaultMaxConcurrencyPerGroup(1)                            // 3rd: fallback (default: 1)

    // --- Layer 1 & 2: In-flight limits ---
    .globalMaxInFlight(100)                                      // Layer 1 cap
    .defaultMaxInFlightPerGroup(20)                              // Layer 2 default
    .perGroupMaxInFlight(Map.of("groupA", 10))                   // Layer 2 per-group override

    // --- Queue thresholds (backpressure) ---
    .globalQueueThreshold(50)                                    // max tasks waiting globally
    .defaultQueueThresholdPerGroup(10)                           // max tasks waiting per group
    .perGroupQueueThreshold(Map.of("groupA", 5))                 // per-group override

    // --- Rejection policy ---
    .rejectionPolicy(RejectionPolicy.ABORT)                      // ABORT | DISCARD | CALLER_RUNS
    // .rejectionHandler(handler)                                 // custom handler (overrides policy)

    // --- Lifecycle listener ---
    .taskLifecycleListener(listener)
    .build();
```

**Concurrency resolution priority:** `perGroupMaxConcurrency` > `concurrencyResolver` > `defaultMaxConcurrencyPerGroup`. Any non-positive value is coerced to 1. Concurrency is resolved once per group on first use ("first resolution fixed" strategy).

**Queue threshold resolution priority:** `perGroupQueueThreshold` > `defaultQueueThresholdPerGroup`. Values < 0 are coerced to 0. A value of 0 means no waiting is allowed -- tasks that cannot immediately acquire a concurrency permit are rejected.

Builder performs **defensive copies** of all mutable Map arguments.

### GroupExecutor

```java
// Create executor
GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy);

// Submit a single task -- returns a TaskHandle
TaskHandle<String> handle = executor.submit("groupA", "task-1", () -> "result");
GroupResult<String> result = handle.await();

// Batch execution -- blocks until all complete, no fail-fast
List<GroupResult<String>> results = executor.executeAll(List.of(
    new GroupTask<>("g1", "t1", () -> "one"),
    new GroupTask<>("g2", "t2", () -> "two")
));

// Per-group management
executor.shutdownGroup("groupA");   // Shut down one group's executor
executor.evictGroup("groupA");      // Evict executor + semaphores + bulkhead + queue

// Graceful shutdown with timeout
boolean allDone = executor.shutdown(Duration.ofSeconds(10));

// Or use try-with-resources (calls shutdown())
executor.close();
```

### TaskHandle

Wraps `Future<GroupResult<T>>` with ergonomic methods:

```java
TaskHandle<String> handle = executor.submit("g", "t1", () -> "hello");

// Blocking wait
GroupResult<String> r = handle.await();                          // throws InterruptedException
GroupResult<String> r = handle.join();                           // catches & re-interrupts

// Timeout wait (returns CANCELLED result on timeout; does NOT cancel the task)
GroupResult<String> r = handle.await(100, TimeUnit.MILLISECONDS);
GroupResult<String> r = handle.join(100, TimeUnit.MILLISECONDS);

// Cancel the underlying task
handle.cancel(true);

// Bridge to CompletableFuture
CompletableFuture<GroupResult<String>> cf = handle.toCompletableFuture();
```

### GroupResult

Immutable record capturing the outcome:

```java
result.groupKey()      // "groupA"
result.taskId()        // "task-1"
result.status()        // SUCCESS | FAILED | CANCELLED | REJECTED
result.value()         // non-null on SUCCESS
result.error()         // non-null on FAILED or CANCELLED
result.durationNanos() // execution time in nanoseconds
```

### TaskLifecycleListener

Optional callbacks for observability. Exceptions thrown by listeners are silently caught.

```java
TaskLifecycleListener listener = new TaskLifecycleListener() {
    @Override
    public void onSubmitted(String groupKey, String taskId) {
        log.info("Submitted {}/{}", groupKey, taskId);
    }
    @Override
    public void onStarted(String groupKey, String taskId) {
        log.info("Started {}/{}", groupKey, taskId);
    }
    @Override
    public void onCompleted(String groupKey, String taskId, GroupResult<?> result) {
        log.info("Completed {}/{} -> {}", groupKey, taskId, result.status());
    }
    @Override
    public void onRejected(String groupKey, String taskId, String reason) {
        log.warn("Rejected {}/{}: {}", groupKey, taskId, reason);
    }
};
```

---

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Task throws any exception | Caught, recorded in `FAILED` result |
| Interrupted during semaphore acquire | Interrupt flag restored, `CANCELLED` result |
| Interrupted during task execution | Interrupt flag restored, `CANCELLED` result |
| `cancel(true)` called on handle | Future cancelled, virtual thread interrupted |
| `await(timeout)` / `join(timeout)` times out | `CANCELLED` result with `TimeoutException` (task continues) |
| Queue threshold exceeded (ABORT) | `RejectedTaskException` thrown; `executeAll()` collects as `REJECTED` result |
| Queue threshold exceeded (DISCARD) | `REJECTED` result returned silently |
| Queue threshold exceeded (CALLER_RUNS) | Task executed directly in the virtual thread, bypassing concurrency permits |
| Custom `RejectionHandler` configured | Handler called on rejection (overrides `RejectionPolicy`) |
| Listener throws exception | Silently caught and ignored |
| Concurrency resolver throws exception | Falls back to `defaultMaxConcurrencyPerGroup` |
| `submit()` / `executeAll()` after shutdown | `IllegalStateException` |

---

## Examples

### Limit concurrency per group

```java
GroupPolicy policy = GroupPolicy.builder()
    .perGroupMaxConcurrency(Map.of("group-1", 2))
    .build();

try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
    for (int i = 0; i < 8; i++) {
        executor.submit("group-1", "t-" + i, () -> {
            // At most 2 tasks run concurrently in group-1
            Thread.sleep(100);
            return null;
        });
    }
}
```

### Global in-flight limit across groups

```java
GroupPolicy policy = GroupPolicy.builder()
    .defaultMaxConcurrencyPerGroup(10)
    .globalMaxInFlight(4)  // Only 4 tasks in-flight across ALL groups
    .build();

try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
    for (String group : List.of("A", "B", "C")) {
        for (int i = 0; i < 4; i++) {
            executor.submit(group, group + "-" + i, () -> {
                Thread.sleep(200);
                return null;
            });
        }
    }
} // 12 total tasks but at most 4 execute globally at any time
```

### Per-group bulkhead (in-flight limit)

```java
GroupPolicy policy = GroupPolicy.builder()
    .defaultMaxConcurrencyPerGroup(4)
    .defaultMaxInFlightPerGroup(3)  // Only 3 tasks queued + running per group
    .build();
```

### Queue threshold with backpressure

```java
GroupPolicy policy = GroupPolicy.builder()
    .defaultMaxConcurrencyPerGroup(2)
    .defaultQueueThresholdPerGroup(3) // at most 3 tasks waiting per group
    .rejectionPolicy(RejectionPolicy.DISCARD)
    .build();

try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
    for (int i = 0; i < 10; i++) {
        TaskHandle<Void> handle = executor.submit("g", "t-" + i, () -> {
            Thread.sleep(500);
            return null;
        });
        GroupResult<Void> result = handle.join();
        if (result.status() == TaskStatus.REJECTED) {
            System.out.println("Task rejected: " + result.taskId());
        }
    }
}
```

### Custom rejection handler

```java
GroupPolicy policy = GroupPolicy.builder()
    .defaultMaxConcurrencyPerGroup(1)
    .defaultQueueThresholdPerGroup(0)  // no waiting allowed
    .rejectionHandler(new RejectionHandler() {
        @Override
        public <T> GroupResult<T> onRejected(String groupKey, String taskId,
                                              Callable<T> task) {
            log.warn("Rejected {}/{}, returning fallback", groupKey, taskId);
            return GroupResult.rejected(groupKey, taskId);
        }
    })
    .build();
```

### Mixed results with executeAll

```java
List<GroupTask<String>> tasks = List.of(
    new GroupTask<>("g", "ok-1",   () -> "hello"),
    new GroupTask<>("g", "fail-1", () -> { throw new RuntimeException("boom"); }),
    new GroupTask<>("g", "ok-2",   () -> "world")
);

List<GroupResult<String>> results = executor.executeAll(tasks);
// results.get(0): SUCCESS, value="hello"
// results.get(1): FAILED,  error=RuntimeException("boom")
// results.get(2): SUCCESS, value="world"
```

---

## Thread Safety

- `GroupExecutor` is safe for concurrent `submit()` and `executeAll()` calls.
- Semaphore and executor caches use `ConcurrentHashMap` with atomic operations.
- Shutdown is idempotent via `AtomicBoolean`.
- `shutdownGroup()` uses atomic `ConcurrentHashMap.compute()` to prevent race conditions.

## Build & Test

Requires **JDK 21**.

```bash
# Build
mvn -Dmaven.repo.local=./.m2/repository package

# Run all tests
mvn -Dmaven.repo.local=./.m2/repository test

# Run a single test class
mvn -Dmaven.repo.local=./.m2/repository -Dtest=GroupExecutorTest test

# Run a single test method
mvn -Dmaven.repo.local=./.m2/repository -Dtest=GroupExecutorTest#testMaxConcurrencyPerGroup test
```

## License

See [LICENSE](LICENSE) for details.
