# group-concurrency

Java library for grouped concurrency on JDK 21: tasks are partitioned by `groupKey`; groups run in parallel and each group enforces its own max concurrency, computed statically or dynamically.

## Quick Start

**Maven**
```xml
<dependency>
  <groupId>io.github.kobe</groupId>
  <artifactId>group-concurrency</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

**Create policy (resolver + per-group override) and run**
```java
import io.github.kobe.*;
import java.util.List;
import java.util.Map;

GroupPolicy policy = GroupPolicy.builder()
        // Highest priority: explicit overrides
        .perGroupMaxConcurrency(Map.of("groupA", 2))
        // Next: resolver (e.g., VIP gets more permits)
        .concurrencyResolver(key -> key.startsWith("vip:") ? 4 : 1)
        // Fallback when nothing else applies
        .defaultMaxConcurrencyPerGroup(1)
        .build();

try (GroupExecutor executor = GroupExecutor.newVirtualThreadExecutor(policy)) {
    List<GroupTask<String>> tasks = List.of(
            new GroupTask<>("vip:alpha", "t1", () -> "hi vip"),
            new GroupTask<>("groupA", "t2", () -> "hi A"),
            new GroupTask<>("user:plain", "t3", () -> "hi user")
    );
    List<GroupResult<String>> results = executor.executeAll(tasks);
    // inspect GroupResult.status()/value()/error()
}
```

## Design Notes
- **Virtual Threads:** uses `Executors.newVirtualThreadPerTaskExecutor()` for lightweight concurrency.
- **Group gates:** each `groupKey` is bound to a `Semaphore` created with the resolved max concurrency. Semaphore is created once on first use to keep behavior stable.
- **No runtime deps:** only JDK 21; JUnit 5 is test-scoped.

## Dynamic Concurrency Policy
- Priority: (1) `perGroupMaxConcurrency` overrides → (2) `concurrencyResolver` (exceptions fall back to default) → (3) `defaultMaxConcurrencyPerGroup` (defaults to 1).
- Any non-positive concurrency is coerced to 1.
- Concurrency is resolved once per group at first sight; later tasks reuse the same semaphore (documented “first resolution fixed” strategy).
- Semaphores are cached; the library assumes a controllable number of groups and does not auto-evict.

## Thread Safety & Cancellation
- Submissions are thread-safe; semaphores guard per-group limits while groups run in parallel.
- If a task is interrupted during acquire or execution, the interrupt flag is restored and the result is marked `CANCELLED` with the cause recorded.
- `TaskHandle.cancel(true)` delegates to the underlying virtual thread future.

## Build & Test
- Run tests: `mvn -Dmaven.repo.local=./.m2/repository test`
- Package quietly: `mvn -q -Dmaven.repo.local=./.m2/repository package`
