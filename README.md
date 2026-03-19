# group-concurrency

A lightweight Java library for **grouped concurrency control** built on JDK 21 Virtual Threads.

Tasks are partitioned by `groupKey` and groups run in parallel, while per-group concurrency limits and an optional global in-flight cap enforce isolation. Virtual threads are created **only when a task is dispatched** — waiting tasks are stored as lightweight objects in an explicit queue, not as blocked threads.

**Zero runtime dependencies** — only JDK 21; JUnit 5 is test-scoped.

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

## Architecture: Explicit Queue Dispatch

The core design avoids blocking virtual threads on semaphore waits. Instead, tasks that cannot run immediately are stored in a bounded `LinkedBlockingQueue` as lightweight `PendingTask` objects. A virtual thread is created only when a task obtains a concurrency permit and is actually dispatched.

```
              submit("groupA", "t1", task)
                        |
                        v
        +-------------------------------+
        | Layer 1: Global In-Flight     |  tryAcquire() — reject if full
        | (optional)                    |
        | caps total tasks across ALL   |
        | groups (globalMaxInFlight)    |
        +-------------------------------+
                        |
                        v
        +-------------------------------+
        | Per-Group Concurrency         |  tryAcquire() the fair Semaphore
        | Semaphore (maxConcurrency)    |
        +-------------------------------+
              /                   \
        permit acquired         no permit
             |                      |
             v                      v
     +----------------+    +---------------------+
     | Dispatch:      |    | Queue (bounded):    |
     | create virtual |    | offer() to          |
     | thread & run   |    | LinkedBlockingQueue |
     +----------------+    +---------------------+
             |                      |
             |               queue full?
             |              /          \
             |          no (queued)   yes → reject
             |              |
             |              v
             |      tryDispatch() closes
             |      the race window
             |
             v
        Task completes
             |
             v
     +---------------------------+
     | onTaskDone():             |
     | poll queue → hand off     |
     | permit to next task       |
     | (no release/acquire       |
     |  round-trip)              |
     | OR release permit +       |
     | tryDispatch()             |
     +---------------------------+
```

**Hand-off dispatch:** When a task completes, the per-group concurrency permit is transferred directly to the next queued task — no release/acquire round-trip. `tryDispatch()` closes the race window between submission and completion.

**Global permits** are released unconditionally in `onTaskDone()`, regardless of hand-off. Each task independently holds its own global permit from submission until completion.

| Component | Scope | Default | Purpose |
|-----------|-------|---------|---------|
| Layer 1: Global In-Flight | Global | `Integer.MAX_VALUE` (off) | Cap total tasks across all groups |
| Per-Group Concurrency | Per-group | `1` | Limit actual concurrent execution per group |
| Per-Group Queue | Per-group | `Integer.MAX_VALUE` (unbounded) | Buffer tasks waiting for a concurrency permit |

---

## Core API

### GroupPolicy

Configure concurrency limits via the builder:

```java
GroupPolicy policy = GroupPolicy.builder()
    // --- Per-group concurrency (resolution priority: high -> low) ---
    .perGroupMaxConcurrency(Map.of("groupA", 2, "groupB", 5))  // 1st: explicit overrides
    .concurrencyResolver(key -> key.startsWith("vip:") ? 4 : 1) // 2nd: dynamic resolver
    .defaultMaxConcurrencyPerGroup(1)                            // 3rd: fallback (default: 1)

    // --- Global in-flight limit ---
    .globalMaxInFlight(100)                                      // cap across all groups

    // --- Queue capacity (backpressure) ---
    .defaultQueueThresholdPerGroup(10)                           // max tasks waiting per group
    .perGroupQueueThreshold(Map.of("groupA", 5))                 // per-group override

    // --- Rejection policy ---
    .rejectionPolicy(RejectionPolicy.ABORT)                      // ABORT | DISCARD | CALLER_RUNS
    // .rejectionHandler(handler)                                 // custom handler (overrides policy)

    // --- Lifecycle listener ---
    .taskLifecycleListener(listener)
    .build();
```

**Concurrency resolution priority:** `perGroupMaxConcurrency` > `concurrencyResolver` > `defaultMaxConcurrencyPerGroup`. Non-positive values from a dynamic `concurrencyResolver` are coerced to 1 at resolution time. Concurrency is resolved once per group on first use ("first resolution fixed" strategy).

**Queue threshold resolution priority:** `perGroupQueueThreshold` > `defaultQueueThresholdPerGroup`. A value of 0 means no queuing allowed — tasks that cannot immediately acquire a concurrency permit are rejected.

**Builder validation:** The builder throws `IllegalArgumentException` on `build()` for invalid values: concurrency < 1, globalMaxInFlight < 1, or queueThreshold < 0. Builder performs **defensive copies** of all mutable Map arguments.

### GroupExecutor

```java
// Create executor
GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy);

// Submit a single task — returns a TaskHandle
TaskHandle<String> handle = executor.submit("groupA", "task-1", () -> "result");
GroupResult<String> result = handle.await();

// Batch execution — blocks until all complete, no fail-fast
List<GroupResult<String>> results = executor.executeAll(List.of(
    new GroupTask<>("g1", "t1", () -> "one"),
    new GroupTask<>("g2", "t2", () -> "two")
));

// Per-group management
executor.shutdownGroup("groupA");   // Drain queue & evict state
executor.evictGroup("groupA");      // Evict with write lock + remove lock entry

// Graceful shutdown with timeout
boolean allDone = executor.shutdown(Duration.ofSeconds(10));

// Or use try-with-resources (calls shutdown())
executor.close();
```

### TaskHandle

Wraps `CompletableFuture<GroupResult<T>>` with ergonomic methods:

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
result.durationNanos() // pure execution time in nanoseconds (excludes queuing/permit wait)
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
| Interrupted during task execution | Interrupt flag restored, `CANCELLED` result |
| `cancel(true)` called on handle | Future cancelled; queued tasks skipped at dispatch if already cancelled |
| `await(timeout)` / `join(timeout)` times out | `CANCELLED` result with `TimeoutException` (task continues) |
| Global in-flight limit exceeded | Rejected immediately (not queued) |
| Per-group queue full (ABORT) | `RejectedTaskException` thrown at `await()` time |
| Per-group queue full (DISCARD) | `REJECTED` result returned silently |
| Per-group queue full (CALLER_RUNS) | All held permits released first, then task executed in caller's thread |
| Custom `RejectionHandler` configured | Handler called on rejection (overrides `RejectionPolicy`); permits released first |
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
            // Remaining tasks wait in the queue
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
    .globalMaxInFlight(4)  // Only 4 tasks accepted across ALL groups
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
} // 12 total tasks but at most 4 in-flight globally at any time
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
    .defaultQueueThresholdPerGroup(0)  // no queuing allowed
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
- Per-group state uses `ConcurrentHashMap` with `computeIfAbsent` ("first resolution fixed").
- `evictGroup()` uses a per-group `ReentrantReadWriteLock` (write lock) to atomically evict all resources; `submit()` uses read locks for consistency. Lock entries are removed after eviction to prevent unbounded growth.
- Shutdown is idempotent via `AtomicBoolean`. `shutdown()` clears all caches; running tasks hold direct semaphore references and release permits on completion.
- All semaphores use fair mode to prevent starvation.
- `onTaskDone()` runs in a `finally` block to guarantee permit release even if an `Error` is thrown.
- `CALLER_RUNS` executes in the caller's thread without holding any permits.

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
