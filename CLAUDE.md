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

This is a **grouped concurrency control library** (`io.github.kobe:group-concurrency`) built on JDK 21 Virtual Threads. It solves the problem of running tasks in parallel across groups while enforcing per-group concurrency limits.

### Core Design: Virtual Threads + Per-Group Semaphores

Tasks are partitioned by `groupKey`. Different groups execute in parallel (via `Executors.newVirtualThreadPerTaskExecutor()`), while a per-group `Semaphore` caps how many tasks within the same group run concurrently.

### Key Classes (all in `io.github.kobe`)

- **`GroupExecutor`** — Entry point. Factory method `newVirtualThreadExecutor(GroupPolicy)`. Submits individual tasks (`submit()`) or batches (`executeAll()`). Implements `AutoCloseable`.
- **`GroupPolicy`** — Builder-configured concurrency policy with a three-tier resolution for each group's max concurrency:
  1. `perGroupMaxConcurrency` — explicit `Map<String, Integer>` overrides (highest priority)
  2. `concurrencyResolver` — `ToIntFunction<String>` for dynamic computation (exceptions fall through)
  3. `defaultMaxConcurrencyPerGroup` — fallback default, minimum 1
- **`GroupTask<T>`** — Immutable record: `(groupKey, taskId, Callable<T>)`
- **`GroupResult<T>`** — Immutable record with status, value/error, and nanosecond timing
- **`TaskHandle<T>`** — Wraps `Future<GroupResult<T>>` with `await()`, `join()`, `cancel()`
- **`TaskStatus`** — Enum: `SUCCESS`, `FAILED`, `CANCELLED`

### Internal

- **`internal.GroupSemaphoreManager`** — Lazily creates and caches `Semaphore` instances per group in a `ConcurrentHashMap`. Uses "first resolution fixed" strategy: once a semaphore is created for a group key, its permit count never changes.

### Cancellation & Thread Safety

- Interrupts during `acquire()` or task execution restore the interrupt flag and produce a `CANCELLED` result.
- Task submission, semaphore access, and executor state (`AtomicBoolean`) are all thread-safe.
