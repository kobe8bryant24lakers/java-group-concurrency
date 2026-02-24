# group-concurrency 设计文档

## 1. 问题定义

在许多业务场景中，需要并行执行大量任务，但不同任务属于不同的逻辑分组（如租户、API 类型、优先级等级），且每组的并发度需要独立控制。例如：

- VIP 用户允许 4 个并发请求，普通用户仅允许 1 个
- 数据库写入组最大 2 并发，读取组最大 8 并发
- 不同 API endpoint 各自限流，同时防止单组任务量暴增拖垮全局

核心语义：**按 groupKey 分组；组间并行；组内也并行，但组内最大并发度可控（1..N）；单组或全局的在途任务数可独立设上限；超限任务可配置拒绝策略。**

## 2. 技术选型

| 关注点 | 选型 | 理由 |
|--------|------|------|
| 线程模型 | JDK 21 Virtual Threads（per-group） | 轻量级，per-group 独立执行器提供逻辑故障隔离和独立生命周期 |
| 并发控制 | `java.util.concurrent.Semaphore`（公平模式） | 精确控制许可数，fair=true 防止饥饿，acquire/release 语义天然匹配 |
| 缓存结构 | `ConcurrentHashMap` | 线程安全的惰性创建与缓存 |
| 并发度配置 | 三级优先级策略 | 兼顾静态覆盖、动态计算和默认值的灵活性 |
| 资源隔离 | `ReentrantReadWriteLock`（per-group） | 保证 evictGroup 与 submit 之间的资源一致性 |
| 运行时依赖 | 无（纯 JDK 21） | 最小化引入，作为库不绑定任何第三方 |
| 测试框架 | JUnit 5 | 仅测试作用域 |

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
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Layer 1: Global In-Flight Semaphore (fair)                 │  │
│  │  globalMaxInFlight（默认 Integer.MAX_VALUE = 无限制）         │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                  │
│  ┌────────────────────────────▼───────────────────────────────┐  │
│  │  GroupExecutorManager — Per-Group VirtualThread Executors   │  │
│  │  "vip" → newVirtualThreadPerTaskExecutor()                  │  │
│  │  "std" → newVirtualThreadPerTaskExecutor()   ...            │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                  │
│  ┌────────────────────────────▼───────────────────────────────┐  │
│  │  GroupBulkheadManager — Layer 2: Per-Group In-Flight (fair) │  │
│  │  "vip" → Semaphore(maxInFlight, fair)                       │  │
│  │  "std" → Semaphore(maxInFlight, fair)   ...                 │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                  │
│  ┌────────────────────────────▼───────────────────────────────┐  │
│  │  GroupSemaphoreManager — Layer 3: Per-Group Concurrency     │  │
│  │  "vip" → Semaphore(4, fair)                                 │  │
│  │  "std" → Semaphore(1, fair)   ...                           │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  GroupQueueManager — Per-Group Queue Threshold Semaphore    │  │
│  │  （仅在配置了 queueThreshold 时创建）                          │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  GroupPolicy — 配置中心                                      │  │
│  │  并发度 / 在途上限 / 队列阈值 / 拒绝策略 / 生命周期监听器       │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│              GroupResult<T> / TaskHandle<T>                       │
│  status: SUCCESS / FAILED / CANCELLED / REJECTED                  │
│  value / error / startTimeNanos / endTimeNanos                    │
└──────────────────────────────────────────────────────────────────┘
```

## 4. 三层防护机制

任务从提交到执行依次通过三层 Semaphore 保护。所有 Semaphore 均使用 `fair=true`（公平模式）以防止饥饿。

| 层级 | 名称 | 作用 | Semaphore 许可数 | 默认值 |
|------|------|------|-----------------|--------|
| Layer 1 | Global In-Flight | 全局在途任务总量上限 | `globalMaxInFlight` | `Integer.MAX_VALUE` |
| Layer 2 | Per-Group Bulkhead | 单组在途任务上限，防止单组资源爆炸 | `resolveMaxInFlight(groupKey)` | `Integer.MAX_VALUE` |
| Layer 3 | Per-Group Concurrency | 单组实际并发执行数上限 | `resolveConcurrency(groupKey)` | 由策略决定，最小为 1 |

**获取顺序**：Global → Bulkhead → Concurrency（固定顺序，杜绝死锁）

**释放顺序**：Concurrency → Bulkhead → Global（逆序，仅释放已成功获取的许可）

**队列阈值**：Layer 1 和 Layer 2/3 各有可选的队列阈值 Semaphore。任务无法立即获取 permit 时，先非阻塞检查队列阈值：
- 队列阈值未满 → 占用一个队列许可，阻塞等待 → 获取成功后释放队列许可
- 队列阈值已满 → 立即释放所有已持有的 permits，调用拒绝处理逻辑

## 5. 类设计

### 5.1 公共 API（`io.github.kobe`）

#### GroupExecutor

核心执行器，对外唯一入口。私有构造器，通过静态工厂方法创建。

```
GroupExecutor                               [implements AutoCloseable]
├── static newVirtualThreadExecutor(GroupPolicy) → GroupExecutor
├── <T> submit(groupKey, taskId, Callable<T>) → TaskHandle<T>
├── <T> executeAll(List<GroupTask<T>>) → List<GroupResult<T>>
├── shutdownGroup(groupKey)                 // 原子关停单组执行器
├── evictGroup(groupKey)                    // 驱逐单组所有缓存资源
├── shutdown()                              // 立即关闭，清理所有缓存
├── shutdown(Duration timeout) → boolean    // 优雅关闭，超时后强制
└── close()                                 // 委托 shutdown()
```

**关键实现细节：**

- `submit()` 在 per-group 读锁保护下获取组执行器，将任务包装为 `executeWithIsolation()` 提交至虚拟线程
- `executeWithIsolation()` 在 per-group 读锁保护下获取三层 Semaphore 引用（确保与 `evictGroup` 的一致性），然后执行三层获取逻辑
- `evictGroup()` 持 per-group 写锁，原子地驱逐执行器、Semaphore、Bulkhead、QueueSemaphore 四类缓存
- `executeAll()` 先批量提交所有任务（全部投递虚拟线程），再逐一 `await()` 收集结果，**不 fail-fast**；仅当调用线程自身被中断时，取消剩余任务
- 被拒绝任务在调用 rejection handler/policy **之前**必须释放所有已持有的 permits
- `AtomicBoolean closed` 保证 `shutdown()` 的幂等性
- 生命周期事件（submitted / started / completed / rejected）在相应节点触发，捕获并静默忽略 `RuntimeException`

#### GroupPolicy

并发策略与隔离策略的聚合配置对象，通过 Builder 模式构建，构建后不可变。

```
GroupPolicy
├── resolveConcurrency(groupKey) → int (≥1)        // 三级解析
├── resolveMaxInFlight(groupKey) → int (≥1)        // 两级解析
├── globalMaxInFlight() → int
├── resolveQueueThreshold(groupKey) → int (≥0)     // 两级解析
├── globalQueueThreshold() → int
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

**在途上限两级解析优先级：**

```
1. perGroupMaxInFlight.get(groupKey)    ← 显式 Map 覆盖
2. defaultMaxInFlightPerGroup           ← 默认值（默认 Integer.MAX_VALUE）
```

**Builder 构建时验证**（`build()` 抛出 `IllegalArgumentException`）：
- `defaultMaxConcurrencyPerGroup` < 1
- `globalMaxInFlight` < 1 / `defaultMaxInFlightPerGroup` < 1
- `globalQueueThreshold` < 0 / `defaultQueueThresholdPerGroup` < 0
- `perGroupMaxConcurrency` 各值 < 1
- `perGroupMaxInFlight` 各值 < 1
- `perGroupQueueThreshold` 各值 < 0

**Builder 防御性拷贝**：三个 Map 参数（`perGroupMaxConcurrency`、`perGroupMaxInFlight`、`perGroupQueueThreshold`）在 Builder setter 和构建时均做防御性拷贝，最终存储为 `unmodifiableMap`。

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

**`startTimeNanos` 在三层 permits 全部获取后才捕获**，因此 `durationNanos()` 反映纯执行时间，不含排队等待时间。

#### TaskHandle\<T\>

已提交任务的句柄，包装 `Future<GroupResult<T>>`，包级私有构造器。

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
- `toCompletableFuture()`：通过一个虚拟线程桥接 `Future` 到 `CompletableFuture`

#### TaskStatus

```
enum TaskStatus { SUCCESS, FAILED, CANCELLED, REJECTED }
```

#### RejectionPolicy

```
enum RejectionPolicy {
    ABORT,        // 抛出 RejectedTaskException（默认）
    DISCARD,      // 静默返回 REJECTED 状态的 GroupResult
    CALLER_RUNS   // 在组执行器的虚拟线程（executeWithIsolation 的当前线程）中执行任务，
                  // 不持有任何 permits，不受并发控制约束
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
    default void onStarted(String groupKey, String taskId) {}         // 所有 permits 获取后
    default void onCompleted(String groupKey, String taskId, GroupResult<?> result) {}
    default void onRejected(String groupKey, String taskId, String reason) {}
}
```

- 所有方法提供 default 空实现
- `GroupExecutor` 调用时捕获并静默忽略 `RuntimeException`

---

### 5.2 内部实现（`io.github.kobe.internal`）

#### GroupSemaphoreManager（Layer 3）

管理 per-group 并发 Semaphore 的惰性创建与缓存。

```
GroupSemaphoreManager
├── semaphoreFor(groupKey) → Semaphore    // computeIfAbsent，fair=true
├── knownGroupCount() → int
├── clear()                               // 清空所有缓存（shutdown 时调用）
└── evict(groupKey)                       // 移除单组缓存（evictGroup 时调用）
```

**"首次解析固定"策略**：groupKey 的 Semaphore 一旦创建，许可数不再改变。即使 `concurrencyResolver` 后续对同一 key 返回不同值，仍复用首次创建的 Semaphore。

#### GroupBulkheadManager（Layer 2）

管理 per-group 在途任务 Semaphore 的惰性创建与缓存。

```
GroupBulkheadManager
├── bulkheadFor(groupKey) → Semaphore     // computeIfAbsent，fair=true
├── clear()
└── evict(groupKey)
```

许可数由 `policy.resolveMaxInFlight(groupKey)` 决定，同样遵循"首次解析固定"策略。

#### GroupQueueManager

管理 per-group 队列阈值 Semaphore 的惰性创建与缓存。供 Layer 2 和 Layer 3 的队列阈值检查共用。

```
GroupQueueManager
├── queueSemaphoreFor(groupKey) → Semaphore   // computeIfAbsent，fair=true
├── clear()
└── evict(groupKey)
```

许可数由 `policy.resolveQueueThreshold(groupKey)` 决定。
`GroupExecutor` 仅在配置了 per-group 队列阈值时才创建此 Manager（`null` 表示无限制）。
全局队列阈值 Semaphore（`globalQueueSemaphore`）直接在 `GroupExecutor` 中管理，仅在 `globalQueueThreshold < Integer.MAX_VALUE` 时创建。

#### GroupExecutorManager

管理 per-group 虚拟线程执行器的生命周期。

```
GroupExecutorManager                       [implements AutoCloseable]
├── executorFor(groupKey) → ExecutorService   // computeIfAbsent，newVirtualThreadPerTaskExecutor
├── shutdownGroup(groupKey)                   // compute() 原子操作：shutdownNow + remove
├── evict(groupKey)                           // 同 shutdownGroup
├── awaitTermination(Duration) → boolean      // 优雅关闭：shutdown() → 等待 → shutdownNow()
└── close()                                   // 立即关闭：shutdownNow() + clear()
```

**`shutdownGroup()` 原子性**：使用 `ConcurrentHashMap.compute()` 确保 shutdownNow 与 remove 的原子性，防止竞态条件。

**`awaitTermination()` 调用约定**：调用方必须先设置 "closed" 标志，防止并发的 `executorFor()` 在等待期间插入新执行器。

## 6. 核心执行流程

### 6.1 单任务提交（submit）

```
调用方 → submit(groupKey, taskId, callable)
    │
    ├── 1. ensureOpen()  ← AtomicBoolean 检查
    ├── 2. 参数非空校验
    ├── 3. fireOnSubmitted(groupKey, taskId)
    ├── 4. 读锁（per-group）
    │      └── executorManager.executorFor(groupKey)
    └── 5. groupExecutor.submit(() -> executeWithIsolation(...))
           → 返回 TaskHandle(groupKey, taskId, future)

虚拟线程内部 executeWithIsolation(groupKey, taskId, callable):
    │
    ├── 0. 读锁（per-group）查找 bulkhead / semaphore / perGroupQueue 引用
    │
    ├── 1. Layer 1 全局在途 Semaphore
    │      tryAcquire() 成功 → 继续
    │      失败 → globalQueueSemaphore != null ?
    │               tryAcquire 失败 → 拒绝（释放已持有 permits → handleRejection）
    │               tryAcquire 成功 → globalInFlightSemaphore.acquire()（阻塞）
    │                                → 释放 globalQueueSemaphore
    │
    ├── 2. Layer 2 per-group 在途 Semaphore（bulkhead）
    │      tryAcquire() 成功 → 继续
    │      失败 → perGroupQueue != null ?
    │               tryAcquire 失败 → 拒绝（释放 global → handleRejection）
    │               tryAcquire 成功 → bulkhead.acquire()（阻塞）
    │                                → 释放 perGroupQueue
    │
    ├── 3. Layer 3 per-group 并发 Semaphore
    │      tryAcquire() 成功 → 继续
    │      失败 → perGroupQueue != null ?
    │               tryAcquire 失败 → 拒绝（释放 bulkhead + global → handleRejection）
    │               tryAcquire 成功 → semaphore.acquire()（阻塞）
    │                                → 释放 perGroupQueue
    │
    ├── 4. startNanos = System.nanoTime()  ← 仅在三层 permits 全部获取后捕获
    ├── 5. fireOnStarted(groupKey, taskId)
    │
    ├── 6. executeTask(groupKey, taskId, callable, startNanos)
    │      ├── callable.call() 成功    → GroupResult.success(...)
    │      ├── InterruptedException   → 恢复中断 + GroupResult.cancelled(...)
    │      ├── CancellationException  → GroupResult.cancelled(...)
    │      └── 其他 Exception         → GroupResult.failed(...)
    │
    ├── 7. fireOnCompleted(groupKey, taskId, result)
    │
    └── finally（逆序释放，仅释放已成功获取的许可）
           semaphore.release() if semaphoreAcquired
           perGroupQueue.release() if perGroupQueueAcquired   ← 理论上此时已释放
           bulkhead.release() if bulkheadAcquired
           globalInFlightSemaphore.release() if globalAcquired
           globalQueueSemaphore.release() if globalQueueAcquired
```

### 6.2 批量执行（executeAll）

```
调用方 → executeAll(List<GroupTask<T>>)
    │
    ├── 1. 遍历所有任务，逐一 submit() → 收集 TaskHandle 列表
    │       （所有任务立即投递到虚拟线程，不等待）
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

### 6.3 拒绝处理（handleRejection）

```
handleRejection(groupKey, taskId, task, reason)
    │
    ├── fireOnRejected(groupKey, taskId, reason)
    │
    ├── rejectionHandler != null ?
    │      → handler.onRejected(groupKey, taskId, task)   ← 优先
    │
    └── rejectionPolicy:
           ABORT        → throw new RejectedTaskException(groupKey, taskId)
           DISCARD      → GroupResult.rejected(groupKey, taskId)
           CALLER_RUNS  → executeTask(...)  ← 在当前虚拟线程执行，不持有任何 permits
```

### 6.4 并发控制示意

```
时间 →

Group "vip" (Layer 3 permits=4, Layer 2 permits=8):
  Task-1: ████████
  Task-2: ████████
  Task-3: ████████
  Task-4: ████████
  Task-5:         ████████   ← Layer 3 等待，最多 queueThreshold 个任务可排队
  Task-6:         ████████

Group "std" (Layer 3 permits=1, Layer 2 permits=2):
  Task-A: ████████████████
  Task-B:                 ████████████████   ← 串行执行
  Task-C:                                 ████████████████

↑ 两个 Group 之间完全并行，互不影响
↑ Layer 1 的 globalMaxInFlight 在两个 Group 之间共享配额
```

## 7. 线程安全分析

| 共享状态 | 保护机制 | 说明 |
|---------|---------|------|
| `GroupExecutor.closed` | `AtomicBoolean` | shutdown 幂等，submit 时检查 |
| `GroupExecutor.groupLocks` | `ConcurrentHashMap` 自身 + per-key `ReentrantReadWriteLock` | `evictGroup` 持写锁，`submit` / `executeWithIsolation` 持读锁，防止混用新旧资源 |
| `GroupSemaphoreManager.semaphoreByGroup` | `ConcurrentHashMap.computeIfAbsent` | 惰性创建，每个 key 只创建一次 |
| `GroupBulkheadManager.bulkheadByGroup` | `ConcurrentHashMap.computeIfAbsent` | 同上 |
| `GroupQueueManager.queueByGroup` | `ConcurrentHashMap.computeIfAbsent` | 同上 |
| `GroupExecutorManager.executorByGroup` | `ConcurrentHashMap.computeIfAbsent` / `compute` | 惰性创建；`shutdownGroup` 使用 `compute()` 保证原子性 |
| 各层 `Semaphore` | Semaphore 自身线程安全，`fair=true` | acquire/release 原子操作，公平排队防饥饿 |
| `GroupPolicy` 全部字段 | 不可变（构建后只读） | Map 存为 `unmodifiableMap`，Builder 做防御性拷贝 |
| `GroupTask` / `GroupResult` | Java record（天然不可变） | 天然线程安全 |

## 8. 中断与取消语义

| 中断/取消发生点 | 处理方式 |
|---------------|---------|
| Layer 1/2/3 `acquire()` 期间 | 捕获 `InterruptedException` → 恢复中断标记 → 返回 `CANCELLED` → finally 仅释放已获取的 permits |
| `task.call()` 内部 | 捕获 `InterruptedException` → 恢复中断标记 → 返回 `CANCELLED` |
| `task.call()` 抛 `CancellationException` | 返回 `CANCELLED` |
| 队列阈值触发拒绝 | 先释放所有已持有 permits → 调用 `handleRejection` → 按策略处理 |
| `TaskHandle.cancel(true)` | 委托 `Future.cancel(true)` → 中断底层虚拟线程 → 触发上述路径 |
| `await(timeout, unit)` 超时 | 捕获 `TimeoutException` → 返回 `CANCELLED`（error=TimeoutException），不取消底层任务 |
| `executeAll` 调用线程被中断 | 当前 `await()` 抛 `InterruptedException` → 恢复中断 → 当前及剩余任务均标记 `CANCELLED` |

## 9. 包结构

```
io.github.kobe
├── GroupExecutor.java             // 核心执行器（入口，三层防护 + per-group 虚拟线程）
├── GroupPolicy.java               // 并发策略 + 隔离策略 + 拒绝策略（Builder，构建时验证）
├── GroupTask.java                 // 任务定义（record，三字段非空）
├── GroupResult.java               // 执行结果（record，含 REJECTED 状态，durationNanos）
├── TaskHandle.java                // 提交句柄（await/join/超时/CompletableFuture）
├── TaskStatus.java                // 状态枚举（SUCCESS/FAILED/CANCELLED/REJECTED）
├── TaskLifecycleListener.java     // 任务生命周期监听器接口（default 空实现）
├── RejectionPolicy.java           // 内置拒绝策略枚举（ABORT/DISCARD/CALLER_RUNS）
├── RejectionHandler.java          // 自定义拒绝处理器（@FunctionalInterface，优先于 RejectionPolicy）
├── RejectedTaskException.java     // ABORT 策略下抛出的异常（含 groupKey/taskId）
└── internal/
    ├── GroupSemaphoreManager.java  // Layer 3: per-group 并发 Semaphore（fair，首次固定）
    ├── GroupBulkheadManager.java   // Layer 2: per-group 在途 Semaphore（fair，首次固定）
    ├── GroupExecutorManager.java   // Per-group 虚拟线程执行器（独立生命周期，compute 原子关停）
    └── GroupQueueManager.java      // Per-group 队列阈值 Semaphore（供 Layer 2/3 共用）

io.github.kobe (test)
├── GroupExecutorTest.java         // 核心功能测试（并发控制、隔离、生命周期、验证等）
└── QueueThresholdTest.java        // 队列阈值与拒绝策略测试
```

## 10. 设计约束与权衡

| 决策 | 选择 | 理由 |
|------|------|------|
| Semaphore 并发度策略 | 首次解析固定 | 简单稳定，避免运行中动态调整许可数带来的复杂性 |
| 缓存清理策略 | 不自动 GC，支持手动 `evictGroup()` | 假设 groupKey 规模可控；手动驱逐给调用方完全控制权 |
| Per-group 执行器 | `newVirtualThreadPerTaskExecutor()`，成本极低 | 实现逻辑故障隔离和独立生命周期（可单独关停），不预分配线程 |
| resolver 异常处理 | 静默降级到默认值 | 单个 resolver 异常不影响整个执行批次 |
| 非法配置值 | Builder `build()` 抛 `IllegalArgumentException` | 尽早暴露配置错误；动态 resolver 返回值通过 `sanitize()` 兜底纠正为 ≥1 |
| fail-fast 策略 | `executeAll` 不 fail-fast | 收集全部结果，调用方可自行决定如何处理失败；仅调用线程中断时才取消剩余 |
| evictGroup 一致性 | per-group `ReentrantReadWriteLock` | 写锁保护 evict，读锁保护资源查找，防止新旧资源混用导致 permit 泄漏 |
| CALLER_RUNS 线程语义 | 在组执行器的虚拟线程中执行，不持有任何 permits | 不阻塞其他任务，任务降级处理时不占用隔离资源 |
| 全局/per-group 队列阈值 | 仅在显式配置时创建对应 Semaphore（null 跳过） | 避免对未启用此功能的场景产生任何额外开销 |
| fair Semaphore 性能 | 轻微额外开销 | 相比非公平模式多一次 FIFO 队列操作；在虚拟线程场景下可忽略，公平性收益更重要 |
