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

This is a **grouped concurrency control library** (`io.github.kobe:group-concurrency`) built on JDK 21 Virtual Threads. It solves the problem of running tasks in parallel across groups while enforcing per-group concurrency limits with three-layer protection.

### Core Design: Virtual Threads + Three-Layer Protection

Tasks are partitioned by `groupKey`. Each group gets its own virtual thread executor for fault isolation. Three layers of semaphores control concurrency:

1. **Layer 1 — Global In-Flight Semaphore** (fair): caps total tasks across all groups
2. **Layer 2 — Per-Group Bulkhead Semaphore** (fair): caps in-flight tasks per group
3. **Layer 3 — Per-Group Concurrency Semaphore** (fair): caps actual concurrency per group

Semaphores are acquired in order (Global → Bulkhead → Concurrency) and released in reverse to prevent deadlocks. Queue threshold checks apply to both Layer 2 and Layer 3 — tasks are rejected before blocking at either layer when thresholds are exceeded. Rejected tasks release all held permits before the rejection handler/policy is invoked.

### Key Classes (all in `io.github.kobe`)

- **`GroupExecutor`** — Entry point. Factory method `newVirtualThreadExecutor(GroupPolicy)`. Submits individual tasks (`submit()`) or batches (`executeAll()`). Implements `AutoCloseable`. Supports `shutdown(Duration)` for graceful shutdown with timeout, `evictGroup(String)` for cache eviction, and `shutdownGroup(String)` for per-group shutdown.
- **`GroupPolicy`** — Builder-configured concurrency policy with:
  - Three-tier concurrency resolution: `perGroupMaxConcurrency` > `concurrencyResolver` > `defaultMaxConcurrencyPerGroup`
  - Isolation config: `globalMaxInFlight`, `defaultMaxInFlightPerGroup`, `perGroupMaxInFlight`
  - Queue threshold config: `globalQueueThreshold`, `defaultQueueThresholdPerGroup`, `perGroupQueueThreshold`
  - Optional `TaskLifecycleListener` for task event callbacks
  - Builder validates all parameters on `build()` — throws `IllegalArgumentException` for invalid values (concurrency < 1, inFlight < 1, queueThreshold < 0)
  - Builder performs defensive copies of mutable Map arguments
- **`GroupTask<T>`** — Immutable record: `(groupKey, taskId, Callable<T>)`
- **`GroupResult<T>`** — Immutable record with status, value/error, and nanosecond timing
- **`TaskHandle<T>`** — Wraps `Future<GroupResult<T>>` with `await()`, `join()`, `cancel()`, `await(timeout, unit)`, `join(timeout, unit)`, and `toCompletableFuture()`
- **`TaskStatus`** — Enum: `SUCCESS`, `FAILED`, `CANCELLED`, `REJECTED`
- **`TaskLifecycleListener`** — Interface with `onSubmitted`, `onStarted`, `onCompleted`, `onRejected` callbacks. Exceptions are silently caught.
- **`RejectionPolicy`** — Enum: `ABORT`, `DISCARD`, `CALLER_RUNS`
- **`RejectionHandler`** — Custom rejection handler interface (overrides `RejectionPolicy` when both are set)
- **`RejectedTaskException`** — Thrown when `ABORT` policy rejects a task

### Internal

- **`internal.GroupSemaphoreManager`** — Layer 3: lazily creates and caches fair `Semaphore` instances per group. Uses "first resolution fixed" strategy. Supports `clear()` and `evict(groupKey)`.
- **`internal.GroupBulkheadManager`** — Layer 2: lazily creates and caches fair `Semaphore` instances per group for in-flight limiting. Supports `clear()` and `evict(groupKey)`.
- **`internal.GroupExecutorManager`** — Manages per-group virtual thread executors. Supports `shutdownGroup()` (atomic via `compute()`), `evict()`, `awaitTermination(Duration)`, and `close()` (uses `shutdownNow()`). Callers must set a "closed" flag before `awaitTermination()`.
- **`internal.GroupQueueManager`** — Manages per-group queue threshold semaphores. Used by Layer 2 and Layer 3 queue threshold checks. Supports `clear()` and `evict(groupKey)`.

### Cancellation & Thread Safety

- Interrupts during `acquire()` or task execution restore the interrupt flag and produce a `CANCELLED` result.
- Task submission, semaphore access, and executor state (`AtomicBoolean`) are all thread-safe.
- `evictGroup()` uses a per-group `ReentrantReadWriteLock` (write lock) for atomicity; `submit()` and `executeWithIsolation()` use read locks for resource lookups.
- `shutdown()` clears all manager caches (executors, semaphores, bulkheads, queues, locks) to allow GC. Running tasks still hold direct semaphore references and release permits on completion.
- `shutdownGroup()` uses atomic `ConcurrentHashMap.compute()` to prevent race conditions.
- All semaphores (Layers 1-3) use fair mode to prevent starvation.
- `CALLER_RUNS` rejection releases all held permits before executing the task.
- `durationNanos()` on `GroupResult` reflects pure execution time (startTimeNanos captured after all permits acquired).
