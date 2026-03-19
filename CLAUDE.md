# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

This is a Maven project targeting **JDK 21**. It uses a local Maven repository (`.m2/`).

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

The project has zero runtime dependencies — only JUnit 5 for testing.

## Architecture

This is a **grouped concurrency control library** (`io.github.kobe:group-concurrency`) built on JDK 21 Virtual Threads. It solves the problem of running tasks in parallel across groups while enforcing per-group concurrency limits with two-layer protection and explicit queue-based scheduling.

### Core Design: Virtual Threads + Explicit Queue Dispatch

Tasks are partitioned by `groupKey`. Virtual threads are created **only when a task is actually dispatched** (i.e., obtains a semaphore permit). Tasks that cannot run immediately are stored in an explicit `LinkedBlockingQueue` as lightweight `PendingTask` objects rather than blocking on a semaphore.

Two layers of control:

1. **Layer 1 — Global In-Flight Semaphore** (optional, fair): caps total in-flight tasks across all groups. Each accepted task holds one global permit from submission until completion. Global permits are released unconditionally in `onTaskDone()`, regardless of hand-off.
2. **Per-Group — Semaphore + LinkedBlockingQueue**: a fair `Semaphore` controls concurrent execution (= `maxConcurrency`), and a bounded `LinkedBlockingQueue` holds waiting tasks. When a task completes, the per-group concurrency permit is handed off directly to the next queued task (no release/acquire round-trip). `tryDispatch()` closes the race window between submission and completion.

Rejected tasks release all held permits before the rejection handler/policy is invoked.

### Key Classes (all in `io.github.kobe`)

- **`GroupExecutor`** — Entry point. Factory method `newVirtualThreadExecutor(GroupPolicy)`. Submits individual tasks (`submit()`) or batches (`executeAll()`). Implements `AutoCloseable`. Supports `shutdown(Duration)` for graceful shutdown with timeout, `evictGroup(String)` for cache eviction (also cleans up per-group lock entries), `shutdownGroup(String)` for per-group shutdown, and observability via `stats()`, `groupStats(String)`, and `activeGroupKeys()`.
- **`GroupPolicy`** — Builder-configured concurrency policy with:
  - Three-tier concurrency resolution: `perGroupMaxConcurrency` > `concurrencyResolver` > `defaultMaxConcurrencyPerGroup`
  - Global config: `globalMaxInFlight`
  - Queue threshold config: `defaultQueueThresholdPerGroup`, `perGroupQueueThreshold`
  - Optional `TaskLifecycleListener` for task event callbacks
  - Builder validates all parameters on `build()` — throws `IllegalArgumentException` for invalid values (concurrency < 1, globalMaxInFlight < 1, queueThreshold < 0, rejectionPolicy null)
  - Builder performs defensive copies of mutable Map arguments
- **`GroupTask<T>`** — Immutable record: `(groupKey, taskId, Callable<T>)`
- **`GroupResult<T>`** — Immutable record with status, value/error, and nanosecond timing
- **`TaskHandle<T>`** — Wraps `CompletableFuture<GroupResult<T>>` with `await()`, `join()`, `cancel()`, `await(timeout, unit)`, `join(timeout, unit)`, and `toCompletableFuture()`
- **`TaskStatus`** — Enum: `SUCCESS`, `FAILED`, `CANCELLED`, `REJECTED`
- **`TaskLifecycleListener`** — Interface with `onSubmitted`, `onStarted`, `onCompleted`, `onRejected` callbacks. RuntimeExceptions are silently caught.
- **`RejectionPolicy`** — Enum: `ABORT`, `DISCARD`, `CALLER_RUNS`
- **`RejectionHandler`** — Custom rejection handler interface (overrides `RejectionPolicy` when both are set)
- **`GroupStats`** — Immutable record: point-in-time snapshot of a single group's state (activeCount, queuedCount, maxConcurrency, queueCapacity)
- **`GroupExecutorStats`** — Immutable record: executor-wide aggregated counters (activeGroupCount, totalActiveCount, totalQueuedCount, globalInFlightUsed, globalInFlightMax)
- **`RejectedTaskException`** — Thrown when `ABORT` policy rejects a task

### Internal

- **`internal.GroupStateManager`** — Manages per-group state (`GroupState` = fair `Semaphore` + `LinkedBlockingQueue`) and dispatch logic (`dispatch`, `onTaskDone`, `tryDispatch`). Uses "first resolution fixed" strategy via `ConcurrentHashMap.computeIfAbsent`. Supports `clear()` and `evict(groupKey)`.

### Cancellation & Thread Safety

- Interrupts during task execution restore the interrupt flag and produce a `CANCELLED` result.
- Task submission, semaphore access, and executor state (`AtomicBoolean`) are all thread-safe.
- `evictGroup()` uses a per-group `ReentrantReadWriteLock` (write lock) for atomicity; `submit()` uses read locks for resource lookups. After eviction, the per-group lock entry is removed from `groupLocks` to prevent unbounded growth.
- `shutdown()` clears all manager caches (state, locks) to allow GC. Running tasks still hold direct semaphore references and release permits on completion.
- All semaphores use fair mode to prevent starvation.
- `CALLER_RUNS` rejection executes the task directly in the thread that called `submit()`, without holding any permits. This matches standard `ThreadPoolExecutor.CallerRunsPolicy` semantics.
- `cancel(true)` on `TaskHandle` marks the `CompletableFuture` as cancelled but does **not** interrupt the underlying virtual thread (`CompletableFuture.cancel()` ignores `mayInterruptIfRunning`). Queued tasks are skipped at dispatch time if their future is already cancelled.
- `onTaskDone()` runs in a `finally` block to guarantee permit release even if an `Error` is thrown.
- `durationNanos()` on `GroupResult` reflects pure execution time (startTimeNanos captured after permit acquisition, before task execution).
