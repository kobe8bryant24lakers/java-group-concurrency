# group-concurrency 设计文档

## 1. 问题定义

在许多业务场景中，需要并行执行大量任务，但不同任务属于不同的逻辑分组（如租户、API 类型、优先级等级），且每组的并发度需要独立控制。例如：

- VIP 用户允许 4 个并发请求，普通用户仅允许 1 个
- 数据库写入组最大 2 并发，读取组最大 8 并发
- 不同 API endpoint 各自限流，同时防止单组任务量暴增拖垮全局

核心语义：**按 groupKey 分组；组间并行；组内也并行，但组内最大并发度可控（1..N）；超限任务可配置排队或拒绝策略。**

### 1.1 现状问题（驱动本次架构演进）

初始实现采用"每任务一虚拟线程 + 信号量阻塞"模式：`submit()` 立即创建一个虚拟线程，该线程在三层 Semaphore 上阻塞等待许可。这带来以下问题：

- **"排队"以阻塞线程形式存在**：提交 100 个任务但只有 2 个并发许可，意味着 98 个虚拟线程对象持续存在并阻塞在信号量上，而任务数据（Callable 等）本可作为轻量对象存储在队列中。
- **资源可见性差**：无法直接查看排队中的任务列表，只能通过信号量等待计数间接反映。
- **无法精确控制虚拟线程创建时机**：虚拟线程在任务提交时创建，而非在真正可以执行时创建。

### 1.2 设计目标

- 每组维护固定数量的并发虚拟线程（= `maxConcurrency`），超出并发数的任务以 Runnable 对象形式存入显式有界队列
- 仅在任务被调度执行时才创建虚拟线程
- 队列容量可独立配置；队列满时按拒绝策略处理

---

## 2. 技术选型

| 关注点 | 选型 | 理由 |
|--------|------|------|
| 线程模型 | JDK 21 Virtual Threads（按需创建，非预分配） | 轻量级，仅在获得调度许可后才创建，避免阻塞态虚拟线程堆积 |
| 并发控制 | `java.util.concurrent.Semaphore`（公平模式） | 精确控制同时执行的虚拟线程数，fair=true 防止饥饿 |
| 任务排队 | `java.util.concurrent.LinkedBlockingQueue` | 显式有界队列，任务以 Runnable 对象形式排队，内存开销远低于阻塞线程 |
| 缓存结构 | `ConcurrentHashMap` | 线程安全的惰性创建与缓存 |
| 并发度配置 | 三级优先级策略 | 兼顾静态覆盖、动态计算和默认值的灵活性 |
| 资源隔离 | `ReentrantReadWriteLock`（per-group） | 保证 evictGroup 与 submit 之间的资源一致性 |
| 运行时依赖 | 无（纯 JDK 21） | 最小化引入，作为库不绑定任何第三方 |
| 测试框架 | JUnit 5 | 仅测试作用域 |

---

## 3. 整体架构

```
┌──────────────────────────────────────────────────────────────────┐
│                           调用方                                  │
│   submit(groupKey, taskId, callable)                              │
│   executeAll(List<GroupTask>)                                     │
│   shutdownGroup(groupKey) / evictGroup(groupKey)                  │
│   shutdown() / shutdown(Duration) / close()                       │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                       GroupExecutor                               │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │  Layer 1: Global In-Flight Semaphore（可选，fair）            │  │
│  │  globalMaxInFlight（默认 Integer.MAX_VALUE = 无限制）          │  │
│  └────────────────────────────┬────────────────────────────────┘  │
│                               │                                   │
│  ┌────────────────────────────▼────────────────────────────────┐  │
│  │  GroupStateManager — Per-Group State                         │  │
│  │                                                              │  │
│  │  "vip" → GroupState {                                        │  │
│  │            Semaphore(4, fair)          ← 并发执行许可         │  │
│  │            LinkedBlockingQueue(8)      ← 排队任务             │  │
│  │          }                                                   │  │
│  │  "std" → GroupState {                                        │  │
│  │            Semaphore(1, fair)                                │  │
│  │            LinkedBlockingQueue(∞)                            │  │
│  │          }                          ...                      │  │
│  │                                                              │  │
│  │  调度逻辑（仅在许可可用时）：                                   │  │
│  │    dispatchVirtualThread() → Thread.ofVirtual().start(task)  │  │
│  │    onTaskDone() → 移交许可给队列中的下一个任务                  │  │
│  │    tryDispatch() → 竞态兜底调度                               │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │  GroupPolicy — 配置中心                                       │  │
│  │  并发度 / 队列容量 / 全局在途 / 拒绝策略 / 生命周期监听器        │  │
│  └─────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                             │
                             ▼（仅在获得许可时才创建虚拟线程）
┌──────────────────────────────────────────────────────────────────┐
│              GroupResult<T> / TaskHandle<T>                       │
│  status: SUCCESS / FAILED / CANCELLED / REJECTED                  │
│  value / error / startTimeNanos / endTimeNanos                    │
└──────────────────────────────────────────────────────────────────┘
```

---

## 4. 两层保护机制

任务从提交到执行经过两层控制。所有 Semaphore 均使用 `fair=true`（公平模式）以防止饥饿。

| 层级 | 名称 | 实现 | 许可数 | 默认值 |
|------|------|------|--------|--------|
| Layer 1 | Global In-Flight | 全局 `Semaphore`（可选）| `globalMaxInFlight` | `Integer.MAX_VALUE`（无限制） |
| Per-Group | 并发 + 显式队列 | `Semaphore` + `LinkedBlockingQueue` | `resolveConcurrency(groupKey)` / `resolveQueueThreshold(groupKey)` | 由策略决定，最小并发 1，默认队列无界 |

### 4.1 调度机制

任务提交时**不创建虚拟线程**，而是：
1. 优先尝试直接获得执行许可（`semaphore.tryAcquire()`）→ 立即创建虚拟线程执行
2. 无许可时尝试入队（`queue.offer(task)`）→ 等待后续调度
3. 队列满时 → 拒绝

任务完成后采用**移交（hand-off）模式**：
- 若队列非空 → 直接将当前许可移交给队首任务，无需 release/acquire 往返
- 若队列为空 → 释放许可，并调用 `tryDispatch()` 处理提交与完成之间的竞态窗口

```
提交 → [semaphore.tryAcquire() 成功] → 立即创建虚拟线程执行
     → [失败] → [queue.offer() 成功] → 等待调度 → tryDispatch()
              → [失败，队列满] → REJECT

完成 → [queue.poll() 非空] → 移交许可，创建虚拟线程执行下一个任务
     → [空] → semaphore.release() → tryDispatch()
```

### 4.2 tryDispatch() 的必要性

`tryDispatch()` 负责消除以下竞态窗口：

**竞态场景**：任务 A 完成并调用 `queue.poll()` 返回空（队列此时确实为空），随后任务 B 的 `submit()` 调用 `semaphore.tryAcquire()` 失败（此时 A 尚未执行 `semaphore.release()`），B 将任务入队。A 随后执行 `semaphore.release()` 并调用 `tryDispatch()`，此时队列中已有 B 的任务，`tryDispatch()` 将其调度。

若无 `tryDispatch()`，B 的任务将永远留在队列中（没有任何任务完成来触发调度）。

### 4.3 Layer 1 与 per-group 的交互

若配置了 `globalMaxInFlight`：
- `submit()` 时先 `tryAcquire()` 全局信号量；失败则直接拒绝（不入队）
- 任务完成后 `onTaskDone()` **无条件释放**完成任务的全局信号量许可（无论是否走移交路径）
- 全局信号量许可在任务排队期间同样被占用（任务入队成功即已持有全局许可）
- 移交路径中：完成任务释放自己的全局许可，队列中的下一个任务继续持有它入队时获得的全局许可——两者独立

---

## 5. 类设计

### 5.1 公共 API（`io.github.kobe`）

#### GroupExecutor

核心执行器，对外唯一入口。私有构造器，通过静态工厂方法创建。

```
GroupExecutor                               [implements AutoCloseable]
├── static newVirtualThreadExecutor(GroupPolicy) → GroupExecutor
├── <T> submit(groupKey, taskId, Callable<T>) → TaskHandle<T>
├── <T> executeAll(List<GroupTask<T>>) → List<GroupResult<T>>
├── shutdownGroup(groupKey)                 // 原子关停单组，清空其队列
├── evictGroup(groupKey)                    // 驱逐单组所有缓存资源
├── shutdown()                              // 立即关闭，清理所有缓存
├── shutdown(Duration timeout) → boolean    // 优雅关闭，等待执行中任务完成
└── close()                                 // 委托 shutdown()
```

**关键实现细节：**

- `submit()` 先检查 closed，再尝试 Layer 1 全局信号量（如有），再通过 `GroupStateManager` 进行 per-group 调度（tryAcquire → dispatch 或 queue.offer → tryDispatch）
- 若队列满或全局信号量耗尽，调用 `handleRejection()` 处理拒绝
- `evictGroup()` 持 per-group 写锁，原子地驱逐 `GroupState`（semaphore + queue）；释放写锁后移除 `groupLocks` 中对应条目，防止长期运行时大量瞬态 groupKey 导致锁 Map 无限增长
- `executeAll()` 先批量提交所有任务，再逐一 `await()` 收集结果，**不 fail-fast**；仅当调用线程自身被中断时，取消剩余任务
- 被拒绝任务在调用 rejection handler/policy **之前**必须释放所有已持有的 permits（包括 Layer 1 全局许可）
- `AtomicBoolean closed` 保证 `shutdown()` 的幂等性
- 生命周期事件（submitted / started / completed / rejected）在相应节点触发，捕获并静默忽略 `RuntimeException`

#### GroupPolicy

并发策略与隔离策略的聚合配置对象，通过 Builder 模式构建，构建后不可变。

```
GroupPolicy
├── resolveConcurrency(groupKey) → int (≥1)        // 三级解析 → Semaphore 许可数
├── resolveQueueThreshold(groupKey) → int (≥0)     // 两级解析 → LinkedBlockingQueue 容量
├── globalMaxInFlight() → int                       // Layer 1 全局信号量许可数
├── taskLifecycleListener() → TaskLifecycleListener
├── rejectionPolicy() → RejectionPolicy
├── rejectionHandler() → RejectionHandler
└── static builder() → Builder
```

**并发度三级解析优先级（从高到低）：**

```
1. perGroupMaxConcurrency.get(groupKey)      ← 显式 Map 覆盖
       │ 存在 → sanitize(value)（≥1）
       │ 不存在 ↓
2. concurrencyResolver.applyAsInt(groupKey)  ← 动态函数计算
       │ 正常返回 → sanitize(result)（≥1）
       │ 抛出异常 → 静默捕获，降级 ↓
3. defaultMaxConcurrencyPerGroup             ← 默认值（默认为 1）
```

**队列容量两级解析优先级：**

```
1. perGroupQueueThreshold.get(groupKey)    ← 显式 Map 覆盖
2. defaultQueueThresholdPerGroup           ← 默认值（默认 Integer.MAX_VALUE = 无界队列）
```

> **语义变化**：`queueThreshold` 在旧设计中是"阻塞前拒绝阈值"（通过额外 Semaphore 实现），在新设计中是 `LinkedBlockingQueue` 的容量上限，语义更直接。

**移除的参数：**
- `defaultMaxInFlightPerGroup`（已废弃）
- `perGroupMaxInFlight`（已废弃）
- `resolveMaxInFlight()`（已废弃）

> 原 in-flight = 并发数 + 排队数。新设计中 in-flight 隐式等于 `maxConcurrency + queueCapacity`，无需单独配置。

**Builder 构建时验证**（`build()` 抛出 `IllegalArgumentException`）：
- `rejectionPolicy` 为 `null`
- `defaultMaxConcurrencyPerGroup` < 1
- `globalMaxInFlight` < 1
- `defaultQueueThresholdPerGroup` < 0
- `perGroupMaxConcurrency` 各值 < 1
- `perGroupQueueThreshold` 各值 < 0

**Builder 防御性拷贝**：Map 参数在 setter 和构建时均做防御性拷贝，最终存储为 `unmodifiableMap`。

#### GroupTask\<T\>

不可变任务定义，Java record。

```
record GroupTask<T>(String groupKey, String taskId, Callable<T> task)
```

- 紧凑构造器校验三字段均非 null

#### GroupResult\<T\>

不可变任务执行结果，Java record。

```
record GroupResult<T>(groupKey, taskId, status, value, error, startTimeNanos, endTimeNanos)
├── durationNanos() → long                  // endTimeNanos - startTimeNanos
├── static success(groupKey, taskId, value, startNanos, endNanos)
├── static failed(groupKey, taskId, error, startNanos, endNanos)
├── static cancelled(groupKey, taskId, error, startNanos, endNanos)
└── static rejected(groupKey, taskId)       // startNanos = endNanos = System.nanoTime()
```

| status | value | error | durationNanos() |
|--------|-------|-------|-----------------|
| SUCCESS | 结果值 | null | 任务执行时间 |
| FAILED | null | 异常 | 任务执行时间 |
| CANCELLED | null | InterruptedException / CancellationException / TimeoutException | 执行时间或 0 |
| REJECTED | null | null | 0 |

**`startTimeNanos` 在 Semaphore 许可获取后、任务执行前捕获**，因此 `durationNanos()` 反映纯执行时间，不含排队等待时间。

#### TaskHandle\<T\>

已提交任务的句柄，包装 `CompletableFuture<GroupResult<T>>`，包级私有构造器。

```
TaskHandle<T>
├── groupKey() / taskId() / future()
├── await() → GroupResult<T>                          [throws InterruptedException]
├── await(timeout, unit) → GroupResult<T>             [throws InterruptedException]
├── join() → GroupResult<T>                           [不抛受检异常]
├── join(timeout, unit) → GroupResult<T>              [不抛受检异常]
├── cancel(mayInterruptIfRunning) → boolean
├── isDone() → boolean
└── toCompletableFuture() → CompletableFuture<GroupResult<T>>
```

- `await()`：等待完成，`CancellationException` 转为 CANCELLED 结果，`ExecutionException` 解包后重抛
- `await(timeout, unit)`：超时返回 CANCELLED 结果（error 为 `TimeoutException`），**不自动取消底层任务**
- `join()` / `join(timeout, unit)`：对应 await 的无受检异常版本，`InterruptedException` 恢复中断标记后返回 CANCELLED 结果
- `toCompletableFuture()`：直接返回内部 `CompletableFuture`（不再需要额外桥接线程）

#### TaskStatus

```
enum TaskStatus { SUCCESS, FAILED, CANCELLED, REJECTED }
```

#### RejectionPolicy

```
enum RejectionPolicy {
    ABORT,        // 抛出 RejectedTaskException（默认）
    DISCARD,      // 静默返回 REJECTED 状态的 GroupResult
    CALLER_RUNS   // 在调用 submit() 的线程中直接执行任务，不持有任何 permits
}
```

#### RejectionHandler

```java
@FunctionalInterface
public interface RejectionHandler {
    <T> GroupResult<T> onRejected(String groupKey, String taskId, Callable<T> task);
}
```

- 自定义拒绝处理器，通过 `GroupPolicy.Builder.rejectionHandler()` 配置
- **优先级高于 `RejectionPolicy`**：两者同时配置时，handler 生效，policy 被忽略

#### RejectedTaskException

```
RejectedTaskException extends RuntimeException
├── groupKey() → String
└── taskId() → String
```

- 仅在 `ABORT` 策略下由 `handleRejection` 抛出

#### TaskLifecycleListener

```java
public interface TaskLifecycleListener {
    default void onSubmitted(String groupKey, String taskId) {}
    default void onStarted(String groupKey, String taskId) {}         // Semaphore 获取后、任务执行前
    default void onCompleted(String groupKey, String taskId, GroupResult<?> result) {}
    default void onRejected(String groupKey, String taskId, String reason) {}
}
```

- 所有方法提供 default 空实现
- `GroupExecutor` 调用时捕获并静默忽略 `RuntimeException`

---

### 5.2 内部实现（`io.github.kobe.internal`）

#### GroupStateManager（新增，取代原 Layer 2/3 管理器）

管理 per-group 并发状态（Semaphore + LinkedBlockingQueue）及调度逻辑。

```
GroupStateManager
├── stateFor(groupKey) → GroupState        // computeIfAbsent，首次解析固定
├── dispatch(state, task, listener)
│     // 创建虚拟线程：执行任务 → fireOnStarted/Completed → onTaskDone()
├── onTaskDone(state, globalSemaphore, listener)
│     // 移交：queue.poll() 非空 → dispatch(next)
│     //        空 → semaphore.release() → tryDispatch()
├── tryDispatch(state, globalSemaphore, listener)
│     // 竞态兜底：queue 非空 && semaphore.tryAcquire() → dispatch(next)
├── evict(groupKey)                        // 移除单组缓存
└── clear()                               // 移除所有缓存（shutdown 时调用）

GroupState（内部类）
├── semaphore: Semaphore(maxConcurrency, fair=true)
├── queue: LinkedBlockingQueue<PendingTask<?>>(queueCapacity)
└── queueCapacity: int                    // 0 表示不允许排队
```

**"首次解析固定"策略**：groupKey 的 `GroupState` 一旦创建，`maxConcurrency` 和 `queueCapacity` 不再改变。

#### 已删除的内部类

| 类 | 原职责 | 替代方案 |
|---|---|---|
| `GroupExecutorManager` | 管理 per-group VirtualThread ExecutorService | 删除，由 `GroupStateManager.dispatch()` 直接创建虚拟线程 |
| `GroupSemaphoreManager` | Layer 3: per-group 并发 Semaphore | 合并入 `GroupState.semaphore` |
| `GroupBulkheadManager` | Layer 2: per-group 在途 Semaphore | 删除，in-flight 由 Semaphore 许可数 + 队列容量隐式控制 |
| `GroupQueueManager` | 队列阈值 Semaphore | 删除，由 `LinkedBlockingQueue` 容量直接控制 |

---

## 6. 核心执行流程

### 6.1 单任务提交（submit）

```
调用方 → submit(groupKey, taskId, callable)
    │
    ├── 1. ensureOpen()  ← AtomicBoolean 检查
    ├── 2. 参数非空校验
    ├── 3. fireOnSubmitted(groupKey, taskId)
    │
    ├── 4. [Layer 1] globalMaxInFlight 已配置？
    │      → tryAcquire(globalInFlightSemaphore)
    │          失败 → fireOnRejected → handleRejection → 返回 REJECTED TaskHandle
    │          成功 → 继续（任务生命周期内持有此许可）
    │
    ├── 5. 读锁（per-group）获取 GroupState
    │
    ├── 6. state.semaphore.tryAcquire()？
    │      成功 → dispatch(state, task, future)  ← 立即创建虚拟线程
    │      失败 →
    │          state.queue.offer(pendingTask)？
    │              成功 → tryDispatch(state, globalSemaphore, listener)  ← 竞态兜底
    │              失败（队列满）→ 释放 Layer 1 许可（如有）
    │                           → fireOnRejected → handleRejection → 返回 REJECTED TaskHandle
    │
    └── 7. 返回 TaskHandle(groupKey, taskId, completableFuture)
```

### 6.2 虚拟线程内执行（dispatch）

```
dispatch(state, task, listener):
    Thread.ofVirtual().start(() -> {
        │
        ├── 1. future.isCancelled()？→ 跳过执行，直接 onTaskDone()
        ├── 2. fireOnStarted(groupKey, taskId)
        ├── 3. startNanos = System.nanoTime()
        │
        ├── 4. 执行任务
        │      callable.call() 成功    → GroupResult.success(...)
        │      InterruptedException   → 恢复中断 + GroupResult.cancelled(...)
        │      CancellationException  → GroupResult.cancelled(...)
        │      其他 Exception         → GroupResult.failed(...)
        │
        ├── 5. fireOnCompleted(groupKey, taskId, result)
        ├── 6. future.complete(result)
        │
        └── 7. finally: onTaskDone(state, globalSemaphore, listener)
    })
```

### 6.3 任务完成后调度（onTaskDone + tryDispatch）

```
onTaskDone(state, globalSemaphore, listener):
    if globalSemaphore != null:
        globalSemaphore.release()          ← 无条件释放完成任务的全局许可
    next = state.queue.poll()
    if next != null:
        dispatch(state, next, listener)    ← 移交：per-group 并发许可不 release/acquire，直接复用
                                           ← next 仍持有它自己入队时获得的全局许可
    else:
        state.semaphore.release()          ← 释放并发许可
        tryDispatch(state, listener)       ← 处理竞态

tryDispatch(state, listener):
    if state.queue.isEmpty() → return
    if !state.semaphore.tryAcquire() → return
    next = state.queue.poll()
    if next == null:
        state.semaphore.release()          ← 队列为空（被其他线程抢先），归还许可
        return
    dispatch(state, next, listener)
```

**竞态正确性**：`tryDispatch()` 在 `submit()` 入队后和 `semaphore.release()` 后各调用一次，确保任何时刻"队列非空 + 许可可用"时，都有线程负责调度。

### 6.4 批量执行（executeAll）

```
调用方 → executeAll(List<GroupTask<T>>)
    │
    ├── 1. 遍历所有任务，逐一 submit() → 收集 TaskHandle 列表
    │       （所有任务立即投递，不等待）
    │
    ├── 2. 遍历 TaskHandle 列表，逐一 await() → 收集 GroupResult
    │       ├── RejectedTaskException → GroupResult.rejected(groupKey, taskId)
    │       └── InterruptedException →
    │               恢复中断标记
    │               当前任务标记为 CANCELLED
    │               剩余 handle.cancel(true) + 标记为 CANCELLED
    │               break
    │
    └── 3. 返回 List<GroupResult<T>>（与输入顺序一致）
```

**不 fail-fast**：某个任务失败或被拒绝，不影响其余任务的执行与收集。

### 6.5 拒绝处理（handleRejection）

```
handleRejection(groupKey, taskId, task)
    │
    ├── rejectionHandler != null ?
    │      → handler.onRejected(groupKey, taskId, task)   ← 优先
    │
    └── rejectionPolicy:
           ABORT        → throw new RejectedTaskException(groupKey, taskId)
           DISCARD      → GroupResult.rejected(groupKey, taskId)
           CALLER_RUNS  → 在调用 submit() 的当前线程中直接执行任务，不持有任何 permits
```

### 6.6 并发控制示意

```
时间 →

Group "vip" (maxConcurrency=4, queueCapacity=8):
  Task-1: ████████
  Task-2: ████████
  Task-3: ████████
  Task-4: ████████
  Task-5: [队列]  ████████   ← 以 PendingTask 对象等待，无虚拟线程创建
  Task-6: [队列]          ████████
  Task-9: REJECTED         ← 4 个运行 + 8 个排队 = 12 个 in-flight，第 13 个拒绝

Group "std" (maxConcurrency=1, queueCapacity=∞):
  Task-A: ████████████████
  Task-B: [队列]          ████████████████   ← 排队，无线程创建
  Task-C: [队列]                          ████████████████

↑ 两个 Group 之间完全并行，互不影响
↑ Layer 1 的 globalMaxInFlight 在两个 Group 之间共享配额
```

---

## 7. 线程安全分析

| 共享状态 | 保护机制 | 说明 |
|---------|---------|------|
| `GroupExecutor.closed` | `AtomicBoolean` | shutdown 幂等，submit 时检查 |
| `GroupExecutor.groupLocks` | `ConcurrentHashMap` 自身 + per-key `ReentrantReadWriteLock` | `evictGroup` 持写锁，`submit` 持读锁，防止混用新旧资源 |
| `GroupStateManager.stateByGroup` | `ConcurrentHashMap.computeIfAbsent` | 惰性创建，每个 key 只创建一次 |
| `GroupState.semaphore` | Semaphore 自身线程安全，`fair=true` | acquire/tryAcquire/release 原子操作，公平排队防饥饿 |
| `GroupState.queue` | `LinkedBlockingQueue` 自身线程安全 | offer/poll 原子操作，`size()` 仅用于近似判断（不依赖其精确性） |
| `GroupPolicy` 全部字段 | 不可变（构建后只读） | Map 存为 `unmodifiableMap`，Builder 做防御性拷贝 |
| `GroupTask` / `GroupResult` | Java record（天然不可变） | 天然线程安全 |
| Layer 1 全局 Semaphore | Semaphore 自身线程安全 | 同 per-group Semaphore |

**`tryDispatch()` 的线程安全**：多个线程可能并发调用 `tryDispatch()`，通过 `semaphore.tryAcquire()` 的原子性保证最多一个线程取得许可并调度。队列的 `poll()` 是原子的，获取许可后 `poll()` 返回 null 表示竞争失败，此时立即归还许可并退出。

---

## 8. 中断与取消语义

| 中断/取消发生点 | 处理方式 |
|---------------|---------|
| `task.call()` 内部 | 捕获 `InterruptedException` → 恢复中断标记 → `future.complete(CANCELLED)` → `onTaskDone()` 正常执行（移交或释放许可） |
| `task.call()` 抛 `CancellationException` | `future.complete(CANCELLED)` → `onTaskDone()` 正常执行 |
| 队列满触发拒绝 | 释放 Layer 1 已持有许可 → 调用 `handleRejection` → 按策略处理 |
| `TaskHandle.cancel(true)` | 委托 `CompletableFuture.cancel(true)` → 将 future 标记为 cancelled；**注意**：`CompletableFuture.cancel()` 不中断底层虚拟线程，正在执行的任务会继续运行直到自行结束，随后 `onTaskDone()` 正常释放许可 |
| `await(timeout, unit)` 超时 | 捕获 `TimeoutException` → 返回 `CANCELLED`（error=TimeoutException），不取消底层任务 |
| 任务在队列中等待时被 cancel | `CompletableFuture.cancel()` 标记 future；任务被调度执行时检查 future 是否已取消，若取消则跳过执行直接调用 `onTaskDone()` |
| `executeAll` 调用线程被中断 | 当前 `await()` 抛 `InterruptedException` → 恢复中断 → 当前及剩余任务均标记 `CANCELLED` |

---

## 9. 兼容性与行为变化

### 9.1 CALLER_RUNS 语义变化（Breaking）

| | 旧设计 | 新设计 |
|---|---|---|
| 执行线程 | `executeWithIsolation` 所在的组虚拟线程 | 调用 `submit()` 的线程（标准 CALLER_RUNS 语义） |
| 原因 | 拒绝发生在虚拟线程内部（信号量阻塞后触发） | 拒绝发生在 `submit()` 调用路径上（队列满时同步拒绝） |
| 影响 | 依赖"CALLER_RUNS 在特定线程执行"的代码需适配 | 更符合 `ThreadPoolExecutor.CallerRunsPolicy` 的直觉语义 |

### 9.2 参数 API 变化（Breaking）

| 参数 | 变化 |
|---|---|
| `defaultMaxInFlightPerGroup` | **移除**（Builder 方法删除） |
| `perGroupMaxInFlight` | **移除**（Builder 方法删除） |
| `queueThreshold`（`defaultQueueThresholdPerGroup`、`perGroupQueueThreshold`） | 语义从"阻塞前的最大等待数（Semaphore 许可）"变为"`LinkedBlockingQueue` 容量"；数值含义一致，无需迁移 |

### 9.3 行为变化（非 Breaking）

| 行为 | 旧设计 | 新设计 |
|---|---|---|
| 提交时虚拟线程创建 | 每次 submit() 立即创建 | 仅在获得调度许可时创建 |
| 排队任务存储 | 虚拟线程对象（阻塞在信号量） | `PendingTask` 对象（LinkedBlockingQueue） |
| 三层 Semaphore | Layer 1 + Layer 2（bulkhead）+ Layer 3（concurrency） | Layer 1（全局）+ Per-Group（Semaphore + 显式队列） |
| `TaskHandle` 内部 Future | `Future<GroupResult<T>>` | `CompletableFuture<GroupResult<T>>` |
| `toCompletableFuture()` | 需额外虚拟线程桥接 | 直接返回内部 CompletableFuture，无额外线程 |
| `shutdownGroup()` | 关停执行器（已提交但排队的虚拟线程继续运行） | 关停后清空队列，运行中任务继续完成 |

---

## 10. 包结构

```
io.github.kobe
├── GroupExecutor.java             // 核心执行器（入口，两层保护 + 调度）
├── GroupPolicy.java               // 并发策略 + 拒绝策略（Builder，构建时验证）
├── GroupTask.java                 // 任务定义（record，三字段非空）
├── GroupResult.java               // 执行结果（record，含 REJECTED 状态，durationNanos）
├── TaskHandle.java                // 提交句柄（await/join/超时/CompletableFuture）
├── TaskStatus.java                // 状态枚举（SUCCESS/FAILED/CANCELLED/REJECTED）
├── TaskLifecycleListener.java     // 任务生命周期监听器接口（default 空实现）
├── RejectionPolicy.java           // 内置拒绝策略枚举（ABORT/DISCARD/CALLER_RUNS）
├── RejectionHandler.java          // 自定义拒绝处理器（@FunctionalInterface，优先于 RejectionPolicy）
└── RejectedTaskException.java     // ABORT 策略下抛出的异常（含 groupKey/taskId）

io.github.kobe.internal
└── GroupStateManager.java         // Per-group 状态管理（Semaphore + Queue + 调度逻辑）

io.github.kobe (test)
├── GroupExecutorTest.java         // 核心功能测试
└── QueueThresholdTest.java        // 队列容量与拒绝策略测试
```

**删除的内部类**：`GroupSemaphoreManager`、`GroupBulkheadManager`、`GroupExecutorManager`、`GroupQueueManager`

---

## 11. 设计约束与权衡

| 决策 | 选择 | 理由 |
|------|------|------|
| 任务创建时机 | 获得许可后才创建虚拟线程 | 避免排队任务以线程形式堆积；PendingTask 对象内存开销远低于虚拟线程对象 |
| 不池化虚拟线程 | 每次调度创建新虚拟线程 | 避免 ThreadLocal 污染（池化线程复用 Thread 对象，前序任务残留 ThreadLocal 值会泄漏给后续任务） |
| Semaphore 并发度策略 | 首次解析固定 | 简单稳定，避免运行中动态调整许可数带来的复杂性 |
| 缓存清理策略 | 不自动 GC，支持手动 `evictGroup()` | 假设 groupKey 规模可控；手动驱逐给调用方完全控制权 |
| 移交（hand-off）模式 | 任务完成后直接传递许可给队列中的下一个任务 | 避免 release + tryAcquire 往返，减少竞态窗口，提升吞吐 |
| tryDispatch() 兜底 | 在 queue.offer() 后和 semaphore.release() 后各调用一次 | 消除两处竞态窗口，确保任何"队列非空 + 许可可用"状态都有调度触发 |
| resolver 异常处理 | 静默降级到默认值 | 单个 resolver 异常不影响整个执行批次 |
| 非法配置值 | Builder `build()` 抛 `IllegalArgumentException` | 尽早暴露配置错误；动态 resolver 返回值通过 `sanitize()` 兜底纠正为 ≥1 |
| fail-fast 策略 | `executeAll` 不 fail-fast | 收集全部结果，调用方可自行决定如何处理失败 |
| CALLER_RUNS 语义 | 在调用 submit() 的线程执行 | 标准语义（同 ThreadPoolExecutor.CallerRunsPolicy），不依赖虚拟线程实现细节 |
| fair Semaphore 性能 | 轻微额外开销 | 相比非公平模式多一次 FIFO 队列操作；公平性收益更重要 |

---

## 12. 测试方案

### 12.1 关键场景

| 场景 | 验证点 |
|---|---|
| 基本并发控制 | maxConcurrency=2，提交 10 个慢任务：同一时刻不超过 2 个并发执行 |
| 队列排队 | maxConcurrency=2，queueCapacity=3，提交 8 个任务：2 运行 + 3 排队 + 3 拒绝 |
| 自动调度 | 运行中任务完成后，队列中的任务自动被调度执行，无需外部触发 |
| 竞态稳定性 | 多线程并发 submit + concurrent complete，验证无任务永久滞留队列 |
| Layer 1 全局限制 | globalMaxInFlight=5，两组各提交 10 个任务：全局同时执行不超过 5 个 |
| 优雅关闭 | shutdown(timeout)：排队中任务被丢弃（REJECTED），运行中任务等待完成 |
| evictGroup | 驱逐后重新提交任务，新的 GroupState 被创建，旧任务不受影响 |
| shutdownGroup | 关停单组不影响其他组 |
| CALLER_RUNS | 队列满时触发，任务在 submit() 调用线程执行，验证线程标识 |
| 生命周期监听器 | 四个回调（submitted/started/completed/rejected）在正确节点触发 |
| 取消排队中的任务 | cancel() 排队任务，最终获得 CANCELLED 结果，许可正确释放 |
| 高并发压力 | 1000+ 任务，50+ 分组，并发提交，验证无死锁/无任务丢失/无许可泄漏 |

### 12.2 兼容性测试

- 原 `testMaxConcurrencyPerGroup` 等并发控制测试：行为不变，正常通过
- 原 `testCallerRunsDoesNotHoldPermits`：改为验证 CALLER_RUNS 在 submit() 调用线程执行

---

## 13. 风险点

| 风险 | 描述 | 缓解措施 |
|------|------|---------|
| tryDispatch() 竞态 | 多线程并发 submit/complete 时，可能出现任务滞留队列 | tryDispatch() 在入队后和 release 后各调用一次；通过高并发压力测试验证 |
| 队列任务取消 | CompletableFuture.cancel() 后任务仍可能被调度到执行 | 调度前检查 future.isCancelled()，跳过执行直接 onTaskDone() |
| Layer 1 与 per-group 的双重控制 | 全局许可被占用但 per-group 队列满，导致全局许可泄漏 | 入队失败时必须释放已持有的 Layer 1 许可；finally 块兜底 |
| evictGroup 期间的调度 | evict 后 GroupState 被移除，但 dispatch 仍持有旧 state 引用 | dispatch 使用局部变量引用 state，evict 只影响新提交；旧任务完成后调用 onTaskDone()，从 stateByGroup 中查找当前（可能是新建的）state |
| shutdown 时的队列任务 | 已入队但未执行的任务可能被忽略 | shutdown() 清空队列前对每个队列任务触发 complete(REJECTED) |
| groupKey 规模膨胀 | 大量瞬态 groupKey 导致 stateByGroup 和 groupLocks 无限增长 | 使用 evictGroup() 清理；文档建议控制 groupKey 规模或按需驱逐 |

---

## 14. 变更日志

### 2026-03-12 — 架构演进：显式队列 + Semaphore 调度

| 变更 | 严重性 | 涉及文件 | 说明 |
|------|--------|---------|------|
| 移除三层 Semaphore，改为显式队列调度 | 架构 | 全部 internal 类、GroupExecutor | 废弃 Layer 2（bulkhead）和 Layer 3（concurrency semaphore）的信号量阻塞模式，改为 `Semaphore` + `LinkedBlockingQueue` 的显式调度：tryAcquire 成功立即创建虚拟线程，失败则入队 |
| 新增 GroupStateManager | 架构 | 新增 `internal/GroupStateManager.java` | 统一管理 per-group Semaphore + LinkedBlockingQueue + 调度逻辑（dispatch / onTaskDone / tryDispatch） |
| 删除 GroupExecutorManager | 架构 | 删除 `internal/GroupExecutorManager.java` | 不再使用 per-group ExecutorService；虚拟线程通过 `Thread.ofVirtual().start()` 按需创建 |
| 删除 GroupSemaphoreManager / GroupBulkheadManager / GroupQueueManager | 架构 | 删除三个文件 | 职责分别由 GroupState.semaphore、LinkedBlockingQueue 容量、GroupStateManager 调度逻辑承担 |
| CALLER_RUNS 执行线程变更 | Breaking | `GroupExecutor.java`, `RejectionPolicy.java` | 从组虚拟线程（executeWithIsolation 线程）改为调用 submit() 的线程，符合标准 CALLER_RUNS 语义 |
| 移除 maxInFlight 相关 API | Breaking | `GroupPolicy.java` | 删除 `defaultMaxInFlightPerGroup`、`perGroupMaxInFlight`、`resolveMaxInFlight()` |
| queueThreshold 语义变更 | 非 Breaking | `GroupPolicy.java` | 从"阻塞前最大等待数（Semaphore 许可）"改为"LinkedBlockingQueue 容量"；数值含义一致 |
| TaskHandle 内部 Future 类型变更 | 非 Breaking | `TaskHandle.java` | 从 `Future<GroupResult<T>>` 改为 `CompletableFuture<GroupResult<T>>`；`toCompletableFuture()` 不再需要额外桥接线程 |

### 2026-03-07 — per-group 队列许可跨层持有 & 拒绝前显式释放许可

| 变更 | 严重性 | 涉及文件 | 说明 |
|------|--------|---------|------|
| per-group 队列许可跨层持有 | 严重 | `GroupExecutor.java` | Layer 2（bulkhead）阻塞等待成功后不立即释放 per-group 队列许可，持续持有至 Layer 3（concurrency）获取完成，防止单调进展被破坏 |
| 拒绝前显式释放已持有的 permits | 严重 | `GroupExecutor.java` | 队列阈值触发拒绝时，在调用 `handleRejection` 之前显式释放已持有的上层 permits |

### 2026-03-06 — 代码审查修复

| 变更 | 严重性 | 涉及文件 | 说明 |
|------|--------|---------|------|
| `submit()` 捕获 `RejectedExecutionException` | 严重 | `GroupExecutor.java` | 底层执行器在 ensureOpen 与 submit 之间被关闭时，捕获异常并返回 REJECTED TaskHandle |
| `evictGroup()` 清理 `groupLocks` 条目 | 中等 | `GroupExecutor.java` | 防止大量瞬态 groupKey 导致锁 Map 无限增长 |
| `CALLER_RUNS` Javadoc 修正 | 中等 | `RejectionPolicy.java` | 修正执行线程描述 |
| Builder 增加 `rejectionPolicy` 空值校验 | 轻微 | `GroupPolicy.java` | build() 新增 null 检查，抛出 IllegalArgumentException |
