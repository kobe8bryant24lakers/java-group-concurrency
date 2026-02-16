# group-concurrency 设计与实现方案

## 1. 问题定义

在许多业务场景中，需要并行执行大量任务，但不同任务属于不同的逻辑分组（如租户、API 类型、优先级等级），且每组的并发度需要独立控制。例如：

- VIP 用户允许 4 个并发请求，普通用户仅允许 1 个
- 数据库写入组最大 2 并发，读取组最大 8 并发
- 不同 API endpoint 各自限流

核心语义：**按 groupKey 分组；组间并行；组内也并行，但组内最大并发度可控（1..N）**。

## 2. 技术选型

| 关注点 | 选型 | 理由 |
|--------|------|------|
| 线程模型 | JDK 21 Virtual Threads | 轻量级，一个任务一个虚拟线程，无需线程池容量规划 |
| 组内限流 | `java.util.concurrent.Semaphore` | 精确控制许可数，acquire/release 语义天然匹配 |
| 缓存结构 | `ConcurrentHashMap` | 线程安全的惰性创建 + 缓存 |
| 并发度配置 | 三级优先级策略 | 兼顾静态配置与动态计算的灵活性 |
| 运行时依赖 | 无（纯 JDK 21） | 最小化引入，作为库不绑定任何第三方 |
| 测试框架 | JUnit 5 | 仅测试作用域 |

## 3. 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                        调用方                                │
│         submit(groupKey, taskId, callable)                   │
│         executeAll(List<GroupTask>)                          │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    GroupExecutor                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ExecutorService (newVirtualThreadPerTaskExecutor)     │   │
│  │  每个 submit → 一个 Virtual Thread                     │   │
│  └──────────────────────────────────────────────────────┘   │
│                         │                                    │
│                         ▼                                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  GroupSemaphoreManager                                │   │
│  │  ConcurrentHashMap<groupKey, Semaphore>               │   │
│  │  惰性创建：首次见到 groupKey 时通过 GroupPolicy 解析    │   │
│  └──────────────────────────────────────────────────────┘   │
│                         │                                    │
│                         ▼                                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  GroupPolicy (三级并发度解析)                           │   │
│  │  perGroupMaxConcurrency → resolver → default          │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              GroupResult<T> / TaskHandle<T>                   │
│  status: SUCCESS / FAILED / CANCELLED                        │
│  value / error / startTimeNanos / endTimeNanos               │
└─────────────────────────────────────────────────────────────┘
```

## 4. 类设计

### 4.1 公共 API（`io.github.kobe`）

#### GroupExecutor

核心执行器，对外入口。

```
GroupExecutor
├── static newVirtualThreadExecutor(GroupPolicy) → GroupExecutor
├── <T> submit(groupKey, taskId, Callable<T>) → TaskHandle<T>
├── <T> executeAll(List<GroupTask<T>>) → List<GroupResult<T>>
├── shutdownGroup(groupKey)
├── evictGroup(groupKey)
├── shutdown() / shutdown(Duration) → boolean
└── close()  [AutoCloseable]
```

**关键实现细节：**

- 私有构造器 + 静态工厂方法，确保只能通过 `newVirtualThreadExecutor` 创建
- 内部持有 `GroupExecutorManager`（per-group virtual-thread executor）、`GroupSemaphoreManager`（Layer 3）、`GroupBulkheadManager`（Layer 2）、`GroupQueueManager`（队列阈值）、`GroupPolicy`
- `AtomicBoolean closed` 保证 shutdown 的幂等性和线程安全
- `submit()` 在 per-group 读锁保护下查找 executor，将任务包装为 `executeWithIsolation()` 投递到虚拟线程执行
- `executeWithIsolation()` 在 per-group 读锁保护下查找 semaphore/bulkhead/queue 资源，确保与 `evictGroup()` 的原子性
- `evictGroup()` 持 per-group 写锁，原子地驱逐所有缓存资源（executor、semaphore、bulkhead、queue）
- `executeAll()` 先批量提交所有任务，再逐一 `await()` 收集结果，**不 fail-fast**
- 被拒绝的任务在调用 rejection handler/policy **之前**释放所有已持有的 permits

#### GroupPolicy

并发度策略配置，通过 Builder 模式构建。

```
GroupPolicy
├── resolveConcurrency(groupKey) → int (≥1)
├── perGroupMaxConcurrency() → Map<String, Integer>
├── concurrencyResolver() → ToIntFunction<String>
├── defaultMaxConcurrencyPerGroup() → int
└── static builder() → Builder
```

**三级解析优先级（从高到低）：**

```
1. perGroupMaxConcurrency.get(groupKey)    ── 显式覆盖（Map）
       │ 存在 → 返回 sanitize(value)
       │ 不存在 ↓
2. concurrencyResolver.applyAsInt(groupKey) ── 动态计算（函数）
       │ 正常 → 返回 sanitize(result)
       │ 异常 → 吞掉，降级 ↓
3. defaultMaxConcurrencyPerGroup            ── 兜底默认值（默认为 1）
```

**健壮性规则：**
- Builder 的 `build()` 方法对直接配置值进行严格验证：concurrency < 1、in-flight < 1、queue threshold < 0 均抛出 `IllegalArgumentException`，尽早暴露配置错误
- 动态 `concurrencyResolver` 返回值通过 `sanitize()` 处理：`raw >= 1 ? raw : 1`，非法值纠正为 1（因为无法控制用户函数的返回值）
- `concurrencyResolver` 抛出异常时静默捕获，降级到默认值，避免整个批次崩溃
- 构造时对 `perGroupMaxConcurrency`、`perGroupMaxInFlight`、`perGroupQueueThreshold` 做防御性拷贝 + `unmodifiableMap`

#### GroupTask\<T\>

不可变任务定义，Java record。

```
record GroupTask<T>(String groupKey, String taskId, Callable<T> task)
```

- 所有字段 non-null（紧凑构造器校验）

#### GroupResult\<T\>

不可变任务执行结果，Java record。

```
record GroupResult<T>(groupKey, taskId, status, value, error, startTimeNanos, endTimeNanos)
├── durationNanos() → long
├── static success(...)
├── static failed(...)
├── static cancelled(...)
└── static rejected(...)
```

- `status` 为 `SUCCESS` 时 `value` 有值、`error` 为 null
- `status` 为 `FAILED`/`CANCELLED` 时 `value` 为 null、`error` 记录原因
- `status` 为 `REJECTED` 时 `value` 和 `error` 均为 null，`durationNanos()` 为 0
- 时间戳使用 `System.nanoTime()`，`startTimeNanos` 在所有 permits 获取之后捕获，因此 `durationNanos()` 反映纯执行时间（不含排队等待时间）

#### TaskHandle\<T\>

已提交任务的句柄，包装 `Future<GroupResult<T>>`。

```
TaskHandle<T>
├── groupKey() → String
├── taskId() → String
├── await() → GroupResult<T>                          [throws InterruptedException]
├── await(timeout, unit) → GroupResult<T>              [throws InterruptedException]
├── join() → GroupResult<T>                            [不抛受检异常]
├── join(timeout, unit) → GroupResult<T>               [不抛受检异常]
├── cancel(mayInterrupt) → boolean
├── isDone() → boolean
└── toCompletableFuture() → CompletableFuture<GroupResult<T>>
```

- `await()`：等待完成，`CancellationException` 转为 CANCELLED 结果，`ExecutionException` 解包
- `await(timeout, unit)`：带超时等待，超时返回 CANCELLED 结果（error 为 `TimeoutException`），不自动取消底层任务
- `join()`：await 的无受检异常版本，中断时返回 CANCELLED 结果并恢复中断标记
- `join(timeout, unit)`：带超时的 join 版本
- `toCompletableFuture()`：通过虚拟线程桥接到 `CompletableFuture`

#### TaskStatus

```
enum TaskStatus { SUCCESS, FAILED, CANCELLED, REJECTED }
```

### 4.2 内部实现（`io.github.kobe.internal`）

#### GroupSemaphoreManager

per-group Semaphore 的生命周期管理。

```
GroupSemaphoreManager
├── semaphoreFor(groupKey) → Semaphore
└── knownGroupCount() → int
```

**实现策略：**

- `ConcurrentHashMap<String, Semaphore>` 存储
- `computeIfAbsent(groupKey, this::createSemaphore)` 实现惰性创建 + 线程安全
- **"首次解析固定"策略**：一个 groupKey 的 Semaphore 一旦创建，许可数不再改变。即使 `concurrencyResolver` 对同一 key 返回不同值，后续调用仍复用首次创建的 Semaphore
- 不自动清理缓存，适用于 groupKey 规模可控的场景

## 5. 核心执行流程

### 5.1 单任务提交（submit）

```
调用方 → submit(groupKey, taskId, callable)
    │
    ├── 1. ensureOpen()  检查执行器未关闭
    ├── 2. 参数非空校验
    ├── 3. executor.submit(() -> executeWithSemaphore(...))
    │       → 启动一个 Virtual Thread
    └── 4. 返回 TaskHandle(groupKey, taskId, future)

Virtual Thread 内部执行：
    executeWithIsolation(groupKey, taskId, callable)
        │
        ├── 0. 读锁保护下查找 bulkhead、semaphore、perGroupQueue
        │
        ├── 1. Layer 1: globalInFlightSemaphore.tryAcquire()
        │       → 成功：继续 │ 失败：检查 global queue threshold → 阻塞等待
        │
        ├── 2. Layer 2: bulkhead.tryAcquire()
        │       → 成功：继续 │ 失败：检查 per-group queue threshold → 阻塞等待
        │       → 被拒绝时先释放 global permit
        │
        ├── 3. Layer 3: semaphore.tryAcquire()
        │       → 成功：继续 │ 失败：检查 per-group queue threshold → 阻塞等待
        │       → 被拒绝时先释放 bulkhead + global permits
        │
        ├── 4. startNanos = System.nanoTime()  ← 仅在所有 permits 获取后捕获
        │
        ├── 5. executeTask(groupKey, taskId, callable, startNanos)
        │       ├── 成功 → GroupResult.success(...)
        │       ├── InterruptedException → 恢复中断 + GroupResult.cancelled(...)
        │       ├── CancellationException → GroupResult.cancelled(...)
        │       └── 其他异常 → GroupResult.failed(...)
        │
        └── 6. finally: 逆序释放 semaphore → bulkhead → global
                → 仅释放已成功 acquire 的许可
```

### 5.2 批量执行（executeAll）

```
调用方 → executeAll(List<GroupTask<T>>)
    │
    ├── 1. 遍历所有任务，逐一 submit() → 收集 TaskHandle 列表
    │       （所有任务立即投递到虚拟线程，不等待）
    │
    ├── 2. 遍历所有 TaskHandle，逐一 await() → 收集 GroupResult
    │       ├── 正常完成 → 加入结果列表
    │       └── 当前线程被中断 →
    │           ├── 恢复中断标记
    │           ├── 当前任务标记为 CANCELLED
    │           ├── 剩余所有 handle.cancel(true)
    │           ├── 剩余全部标记为 CANCELLED
    │           └── break
    │
    └── 3. 返回 List<GroupResult<T>>（与输入顺序一致）
```

**关键行为：不 fail-fast。** 即使某个任务失败，其余任务继续执行并收集结果。只有当调用线程自身被中断时，才取消剩余任务。

### 5.3 并发控制示意

```
时间 →

Group "vip" (Semaphore permits=4):
  Task-1: ████████
  Task-2: ████████
  Task-3: ████████
  Task-4: ████████
  Task-5:         ████████   ← 等待前 4 个中的一个完成后获取许可
  Task-6:         ████████

Group "std" (Semaphore permits=1):
  Task-A: ████████████████
  Task-B:                 ████████████████   ← 串行执行
  Task-C:                                 ████████████████

↑ 两个 Group 之间完全并行，互不影响
```

## 6. 线程安全分析

| 共享状态 | 保护机制 | 说明 |
|---------|---------|------|
| `GroupExecutor.closed` | `AtomicBoolean` | shutdown 幂等，submit 时检查 |
| `GroupExecutor.groupLocks` | `ConcurrentHashMap<String, ReentrantReadWriteLock>` | per-group 读写锁，确保 evictGroup 与 submit/executeWithIsolation 的原子性 |
| `GroupSemaphoreManager.semaphoreByGroup` | `ConcurrentHashMap.computeIfAbsent` | 惰性创建保证每个 key 只创建一次 |
| `GroupBulkheadManager.bulkheadByGroup` | `ConcurrentHashMap.computeIfAbsent` | 同上 |
| `GroupQueueManager.queueByGroup` | `ConcurrentHashMap.computeIfAbsent` | 同上 |
| `GroupExecutorManager.executorByGroup` | `ConcurrentHashMap.computeIfAbsent` / `compute` | 惰性创建 + shutdownGroup 原子操作 |
| 各组的 `Semaphore` | Semaphore 自身线程安全 | acquire/release 原子操作，均使用 fair 模式 |
| `GroupPolicy` 字段 | 不可变（构造后只读） | Map 为 unmodifiableMap，Builder 做防御性拷贝 |
| `GroupTask`, `GroupResult` | Java record（不可变） | 天然线程安全 |

## 7. 中断与取消语义

```
中断发生点                │  处理方式
──────────────────────────┼──────────────────────────────────
Layer 1/2/3 acquire()     │  捕获 InterruptedException
                          │  → 恢复中断标记
                          │  → 返回 GroupResult.cancelled()
                          │  → finally 仅释放已成功 acquire 的许可
──────────────────────────┼──────────────────────────────────
task.call() 内部          │  捕获 InterruptedException
                          │  → 恢复中断标记
                          │  → 返回 GroupResult.cancelled()
                          │  → finally 中释放所有已获取的许可
──────────────────────────┼──────────────────────────────────
queue threshold 拒绝      │  先释放所有已持有的 permits
                          │  → 调用 handleRejection
                          │  → ABORT: 抛 RejectedTaskException
                          │  → DISCARD: 返回 REJECTED 结果
                          │  → CALLER_RUNS: 无 permits 下执行任务
──────────────────────────┼──────────────────────────────────
TaskHandle.cancel(true)   │  委托 Future.cancel(true)
                          │  → 中断底层虚拟线程
                          │  → 触发上述路径之一
──────────────────────────┼──────────────────────────────────
executeAll 调用线程中断    │  当前 await 抛 InterruptedException
                          │  → 恢复中断标记
                          │  → 当前 + 剩余任务标记 CANCELLED
                          │  → 剩余 handle.cancel(true)
```

## 8. 测试策略

4 个核心测试用例（初始），覆盖主要功能维度：

| 测试方法 | 验证目标 | 实现手段 |
|---------|---------|---------|
| `testMaxConcurrencyPerGroup` | 同组并发度上限（perGroup 覆盖 = 2） | AtomicInteger 跟踪峰值 + CountDownLatch 同步起跑 + sleep 拉开窗口 |
| `testParallelAcrossGroups` | 不同组之间真正并行 | 全局 AtomicInteger 峰值 > 单组上限 |
| `testCollectFailures` | executeAll 不 fail-fast + 失败结果正确标记 | 混合成功/失败任务，检查所有结果状态 |
| `testDynamicConcurrencyResolver` | resolver 动态计算并发度 | vip 组（resolver→4）vs 普通组（default→1），分别验证峰值 |

## 9. 包结构

```
io.github.kobe
├── GroupExecutor.java             // 核心执行器（入口）
├── GroupPolicy.java               // 并发策略 + 隔离策略（Builder，含构建时验证）
├── GroupTask.java                 // 任务定义（record）
├── GroupResult.java               // 执行结果（record，含 REJECTED 状态）
├── TaskHandle.java                // 提交句柄（含超时 + CompletableFuture）
├── TaskStatus.java                // 状态枚举（SUCCESS/FAILED/CANCELLED/REJECTED）
├── TaskLifecycleListener.java     // 任务生命周期监听器接口
├── RejectionPolicy.java           // 内置拒绝策略枚举（ABORT/DISCARD/CALLER_RUNS）
├── RejectionHandler.java          // 自定义拒绝处理器接口
├── RejectedTaskException.java     // 队列阈值超限异常
└── internal/
    ├── GroupSemaphoreManager.java  // Layer 3: per-group 并发 Semaphore 管理（fair）
    ├── GroupBulkheadManager.java   // Layer 2: per-group 在途 Semaphore 管理（fair）
    ├── GroupExecutorManager.java   // Per-group 虚拟线程执行器管理
    └── GroupQueueManager.java      // Per-group 队列阈值 Semaphore 管理

io.github.kobe (test)
├── GroupExecutorTest.java         // 28 个测试（含并发控制、隔离、生命周期、验证）
└── QueueThresholdTest.java        // 14 个测试（队列阈值 + 拒绝策略）
```

## 10. 设计约束与权衡

| 决策 | 选择 | 理由 |
|------|------|------|
| Semaphore 缓存策略 | 不自动清理（支持手动 evict） | 库假设 groupKey 规模可控；`evictGroup()` 支持手动释放资源 |
| 并发度变更策略 | 首次解析固定 | 简单稳定，避免运行中 Semaphore 许可数动态调整带来的复杂性和不可预测性 |
| resolver 异常处理 | 静默降级到默认值 | 保证单个 resolver 异常不影响整个执行批次 |
| 非法配置值处理 | Builder 阶段抛出 `IllegalArgumentException` | 尽早暴露配置错误；动态 resolver 返回值仍通过 sanitize 纠正为 1 |
| 执行器类型 | per-group virtual-thread-per-task | 每组独立虚拟线程执行器，提供故障隔离和独立生命周期 |
| fail-fast 策略 | 不 fail-fast | executeAll 收集全部结果，调用方可自行决定如何处理失败 |
| evictGroup 一致性 | per-group ReadWriteLock | 写锁保护 evict，读锁保护资源查找，确保不出现新旧混合资源 |
| 被拒绝任务的 permit 处理 | 先释放再 reject | CALLER_RUNS 不会占用隔离 permit，ABORT/DISCARD 也快速释放资源 |

---

## 11. 重构方案：分组隔离 + 低资源消耗

### 11.1 问题分析

当前 `GroupExecutor` 使用**单一共享** `Executors.newVirtualThreadPerTaskExecutor()` 处理所有分组的任务，仅靠 per-group `Semaphore` 控制并发。存在以下隔离缺陷：

- **无故障隔离**：一个分组的任务如果 pin 住 carrier 线程（如 `synchronized` 或原生 I/O），会拖慢所有分组
- **无资源隔离**：一个分组提交大量任务时，会创建大量虚拟线程占用内存和 carrier 资源
- **无调度公平性**：提交 10000 个任务的分组和提交 1 个任务的分组，在调度上没有区别

**目标**：实现故障隔离、资源隔离、调度公平性，同时保持虚拟线程、零第三方依赖、低资源消耗。

### 11.2 核心设计：三层防护 + Per-Group 虚拟线程执行器

```
┌─────────────────────────────────────────────────┐
│  Layer 1: Global In-Flight Semaphore (fair)     │  ← 系统级总量控制 + 公平调度
│  e.g. globalMaxInFlight = 256                   │
├────────────────────┬────────────────────────────┤
│  Layer 2: Per-Group│  Per-Group Bulkhead (fair) │  ← 分组级资源隔离（限制在途任务数）
│  Virtual Thread    │  e.g. maxInFlight = 32     │
│  Executor          │                            │
├────────────────────┼────────────────────────────┤
│                    │  Layer 3: Per-Group         │  ← 分组级并发控制（已有机制）
│                    │  Concurrency Semaphore      │
│                    │  e.g. maxConcurrency = 4    │
└────────────────────┴────────────────────────────┘
```

**各层职责：**

| 层级 | 职责 | 机制 | 默认值 |
|------|------|------|--------|
| Layer 1 | 全局在途总量上限，保证公平调度 | `Semaphore(globalMax, fair=true)` | `Integer.MAX_VALUE`（无限制） |
| Layer 2 | 单分组在途任务上限，防止单组资源爆炸 | Per-group `Semaphore(maxInFlight, fair=true)` | `Integer.MAX_VALUE`（无限制） |
| Per-Group Executor | 逻辑故障隔离，独立生命周期 | `Executors.newVirtualThreadPerTaskExecutor()` per group | 自动创建 |
| Layer 3 | 单分组实际并发数控制 | Per-group `Semaphore(permits)`（已有） | 由 `GroupPolicy` 三级解析决定 |

**关键设计决策：**

- **公平性保证**：Layer 1 和 Layer 2 使用 `fair=true` 的 Semaphore，确保 FIFO 顺序获取 permit，避免饥饿
- **向后兼容**：所有新字段默认 `Integer.MAX_VALUE`，不配置时行为与现有实现完全一致
- **固定获取顺序**：三层 Semaphore 始终按 Global → Bulkhead → Concurrency 顺序获取、逆序释放，杜绝死锁
- **Per-Group Executor 成本极低**：`newVirtualThreadPerTaskExecutor()` 不预分配线程，本质是虚拟线程工厂包装器

### 11.3 方案评估与选型

在设计过程中评估了四种候选方案：

| 方案 | 描述 | 故障隔离 | 资源隔离 | 公平性 | 资源消耗 | 结论 |
|------|------|---------|---------|--------|---------|------|
| A: Per-Group VT Executor | 每组独立虚拟线程执行器 | 逻辑隔离 ✓ | 弱 | 无 | 极低 | 单用不够 |
| B: Per-Group ForkJoinPool | 每组独立 carrier 线程池 | 物理隔离 ✓✓ | 强 ✓✓ | 无 | 极高 ✗ | 不可行 |
| C: 共享 + 限流 + Bulkhead | 共享执行器 + 令牌桶 + 隔板 | 无 | 中等 | 弱 | 低 | 概念错配 |
| D: 公平调度队列 | per-group 队列 + round-robin 分发 | 弱 | 中等 | 强 ✓✓ | 中等 | 过于复杂 |
| **混合 A+D** | **Per-Group Executor + 三层 Semaphore** | **逻辑隔离 ✓** | **强 ✓✓** | **强 ✓✓** | **低 ✓** | **推荐** |

**选择混合方案的原因：**
- 方案 A 提供逻辑故障隔离和独立生命周期（可单独关停一个分组），成本极低
- Layer 1 + Layer 2 的 fair Semaphore 组合提供资源隔离和公平调度
- 无需引入调度线程或复杂队列，保持"submit and go"的简洁模型

**关于 carrier 线程 pinning**：这是 JVM 层面的问题，库级别无法完全隔离。JDK 24 的 JEP 491 将修复 `synchronized` 导致的 pinning。当前方案通过 per-group executor 限制了逻辑层面的影响范围。

### 11.4 实现步骤

#### Step 1: 扩展 `GroupPolicy` — 新增隔离配置

**文件**: `src/main/java/io/github/kobe/GroupPolicy.java`

新增 3 个字段和对应 Builder 方法：

```java
private final int globalMaxInFlight;                    // Layer 1
private final int defaultMaxInFlightPerGroup;           // Layer 2 默认值
private final Map<String, Integer> perGroupMaxInFlight; // Layer 2 per-group 覆盖
```

新增解析方法：

```java
public int resolveMaxInFlight(String groupKey) {
    Integer overridden = perGroupMaxInFlight.get(groupKey);
    if (overridden != null) return sanitize(overridden);
    return defaultMaxInFlightPerGroup;
}

public int globalMaxInFlight() {
    return globalMaxInFlight;
}
```

Builder 新增方法：
- `globalMaxInFlight(int)` — 全局在途上限
- `defaultMaxInFlightPerGroup(int)` — 分组默认在途上限
- `perGroupMaxInFlight(Map<String, Integer>)` — 分组在途上限覆盖

所有默认值为 `Integer.MAX_VALUE`。

#### Step 2: 新建 `GroupExecutorManager` — 管理 per-group 虚拟线程执行器

**新文件**: `src/main/java/io/github/kobe/internal/GroupExecutorManager.java`

```java
public final class GroupExecutorManager implements AutoCloseable {
    private final ConcurrentHashMap<String, ExecutorService> executorByGroup = new ConcurrentHashMap<>();

    public ExecutorService executorFor(String groupKey) {
        return executorByGroup.computeIfAbsent(groupKey,
            k -> Executors.newVirtualThreadPerTaskExecutor());
    }

    public void shutdownGroup(String groupKey) {
        ExecutorService ex = executorByGroup.remove(groupKey);
        if (ex != null) ex.shutdownNow();
    }

    @Override
    public void close() {
        executorByGroup.values().forEach(ExecutorService::shutdown);
    }
}
```

#### Step 3: 新建 `GroupBulkheadManager` — 管理 per-group 在途 Semaphore

**新文件**: `src/main/java/io/github/kobe/internal/GroupBulkheadManager.java`

参照已有的 `GroupSemaphoreManager` 模式：

```java
public final class GroupBulkheadManager {
    private final GroupPolicy policy;
    private final ConcurrentHashMap<String, Semaphore> bulkheadByGroup = new ConcurrentHashMap<>();

    public Semaphore bulkheadFor(String groupKey) {
        return bulkheadByGroup.computeIfAbsent(groupKey, this::create);
    }

    private Semaphore create(String groupKey) {
        return new Semaphore(policy.resolveMaxInFlight(groupKey), true); // fair=true
    }
}
```

#### Step 4: 重构 `GroupExecutor` — 三层防护 + per-group 执行器

**文件**: `src/main/java/io/github/kobe/GroupExecutor.java`

**字段变更**：

```java
// 替换
- private final ExecutorService executor;
// 为
+ private final GroupExecutorManager executorManager;
+ private final GroupBulkheadManager bulkheadManager;
+ private final Semaphore globalInFlightSemaphore;
```

**工厂方法变更**：

```java
public static GroupExecutor newVirtualThreadExecutor(GroupPolicy policy) {
    return new GroupExecutor(
        policy,
        new GroupExecutorManager(),
        new GroupSemaphoreManager(policy),
        new GroupBulkheadManager(policy),
        new Semaphore(policy.globalMaxInFlight(), true)  // fair=true
    );
}
```

**submit() 变更** — 使用 per-group 执行器：

```java
public <T> TaskHandle<T> submit(String groupKey, String taskId, Callable<T> task) {
    ensureOpen();
    // ...null checks...
    ExecutorService groupExecutor = executorManager.executorFor(groupKey);
    var future = groupExecutor.submit(() -> executeWithIsolation(groupKey, taskId, task));
    return new TaskHandle<>(groupKey, taskId, future);
}
```

**执行方法变更** — 三层 Semaphore 按固定顺序获取/释放（避免死锁），支持队列阈值：

```java
private <T> GroupResult<T> executeWithIsolation(String groupKey, String taskId, Callable<T> task) {
    // 读锁保护下查找 per-group 资源，确保与 evictGroup 的原子性
    Semaphore bulkhead, semaphore, perGroupQueue;
    readLock.lock();
    try { bulkhead = ...; semaphore = ...; perGroupQueue = ...; }
    finally { readLock.unlock(); }

    try {
        // Layer 1: tryAcquire → 失败则 global queue check → blocking acquire
        // Layer 2: tryAcquire → 失败则 per-group queue check → blocking acquire
        //          拒绝时先释放 global permit
        // Layer 3: tryAcquire → 失败则 per-group queue check → blocking acquire
        //          拒绝时先释放 bulkhead + global permits
        long startNanos = System.nanoTime(); // 仅在所有 permits 获取后
        return executeTask(groupKey, taskId, task, startNanos);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        long now = System.nanoTime();
        return GroupResult.cancelled(groupKey, taskId, e, now, now);
    } finally {
        // 逆序释放：semaphore → bulkhead → global（仅释放已成功 acquire 的）
    }
}
```

**新增方法** — 单组关停：

```java
public void shutdownGroup(String groupKey) {
    executorManager.shutdownGroup(groupKey);
}
```

**shutdown()/close() 变更**：

```java
public void shutdown() {
    if (closed.compareAndSet(false, true)) {
        executorManager.close();  // 关闭所有 per-group 执行器
    }
}
```

**executeAll() 和 executeTask()** 不变。

### 11.5 重构后的架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            调用方                                       │
│         submit(groupKey, taskId, callable)                               │
│         executeAll(List<GroupTask>)                                      │
│         shutdownGroup(groupKey)                                         │
└────────────────────────┬────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        GroupExecutor                                     │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │  Layer 1: Global In-Flight Semaphore (fair)                       │ │
│  │  globalMaxInFlight = 256                                          │ │
│  └────────────────────────────────┬──────────────────────────────────┘ │
│                                   │                                     │
│  ┌────────────────────────────────▼──────────────────────────────────┐ │
│  │  GroupExecutorManager (per-group VirtualThread Executors)          │ │
│  │  ┌────────────────────┐  ┌────────────────────┐                   │ │
│  │  │ Group "vip" Executor│  │ Group "std" Executor│  ...             │ │
│  │  └────────────────────┘  └────────────────────┘                   │ │
│  └────────────────────────────────┬──────────────────────────────────┘ │
│                                   │                                     │
│  ┌────────────────────────────────▼──────────────────────────────────┐ │
│  │  GroupBulkheadManager (Layer 2: per-group in-flight Semaphore)     │ │
│  │  "vip" → Semaphore(32, fair)    "std" → Semaphore(16, fair)       │ │
│  └────────────────────────────────┬──────────────────────────────────┘ │
│                                   │                                     │
│  ┌────────────────────────────────▼──────────────────────────────────┐ │
│  │  GroupSemaphoreManager (Layer 3: per-group concurrency Semaphore)  │ │
│  │  "vip" → Semaphore(4)          "std" → Semaphore(1)               │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │  GroupPolicy (配置中心)                                            │ │
│  │  globalMaxInFlight / perGroupMaxInFlight / defaultMaxInFlightPerGroup│
│  │  perGroupMaxConcurrency / concurrencyResolver / default            │ │
│  └───────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              GroupResult<T> / TaskHandle<T>                               │
│  status: SUCCESS / FAILED / CANCELLED / REJECTED                         │
│  value / error / startTimeNanos / endTimeNanos                           │
└─────────────────────────────────────────────────────────────────────────┘
```

### 11.6 文件变更汇总

| 文件 | 操作 | 描述 |
|------|------|------|
| `GroupPolicy.java` | 修改 | 新增 `globalMaxInFlight`, `defaultMaxInFlightPerGroup`, `perGroupMaxInFlight` 字段及 Builder 方法 |
| `GroupExecutor.java` | 修改 | 替换单一执行器为 per-group 执行器 + 三层 Semaphore 获取，新增 `shutdownGroup()` |
| `internal/GroupBulkheadManager.java` | **新建** | Per-group 在途 Semaphore 管理器 |
| `internal/GroupExecutorManager.java` | **新建** | Per-group 虚拟线程执行器管理器 |
| `GroupSemaphoreManager.java` | 不变 | 无需修改 |
| `GroupTask.java` / `GroupResult.java` / `TaskHandle.java` / `TaskStatus.java` | 不变 | 无需修改 |
| `GroupExecutorTest.java` | 修改 | 新增 4 个测试用例，原有 4 个测试不变 |

### 11.7 重构后的包结构

```
io.github.kobe
├── GroupExecutor.java             // 核心执行器（入口）
├── GroupPolicy.java               // 并发策略 + 隔离策略（Builder）
├── GroupTask.java                 // 任务定义（record）
├── GroupResult.java               // 执行结果（record）
├── TaskHandle.java                // 提交句柄
├── TaskStatus.java                // 状态枚举
└── internal/
    ├── GroupSemaphoreManager.java  // Layer 3: per-group 并发 Semaphore 管理
    ├── GroupBulkheadManager.java   // Layer 2: per-group 在途 Semaphore 管理（新）
    └── GroupExecutorManager.java   // Per-group 虚拟线程执行器管理（新）
```

### 11.8 测试策略

**已有测试（不变，验证向后兼容）：**

| 测试方法 | 验证目标 |
|---------|---------|
| `testMaxConcurrencyPerGroup` | 同组并发度上限 |
| `testParallelAcrossGroups` | 不同组之间真正并行 |
| `testCollectFailures` | executeAll 不 fail-fast |
| `testDynamicConcurrencyResolver` | resolver 动态计算并发度 |

**新增测试：**

| 测试方法 | 验证目标 | 关键断言 |
|---------|---------|---------|
| `testPerGroupInFlightBulkhead` | Layer 2 per-group 在途上限 | 提交 20 任务，验证同时在途 ≤ 5 |
| `testGlobalInFlightFairness` | Layer 1 全局上限 + 分组公平性 | Group A 100 任务不饿死 Group B 的 2 任务 |
| `testShutdownGroupDoesNotAffectOtherGroups` | 单组关停故障隔离 | 关停 Group A 后 Group B 仍正常完成 |
| `testUnlimitedDefaultsBehaveLikeOriginal` | 默认值向后兼容 | 不配置新字段时行为与重构前一致 |

### 11.9 验证计划

```bash
# 1. 运行全部已有测试，验证向后兼容
mvn -Dmaven.repo.local=./.m2/repository test

# 2. 实现新测试后，运行全部测试确认通过
mvn -Dmaven.repo.local=./.m2/repository test

# 3. 运行单个新测试（如需调试）
mvn -Dmaven.repo.local=./.m2/repository -Dtest=GroupExecutorTest#testPerGroupInFlightBulkhead test
```

### 11.10 风险与缓解

| 风险 | 缓解措施 |
|------|---------|
| 三层 Semaphore 死锁 | 固定获取顺序 Global → Bulkhead → Concurrency，逆序释放，无环形依赖 |
| Per-group Executor 内存增长 | `ConcurrentHashMap` 惰性创建，与 `GroupSemaphoreManager` 同样假设 groupKey 规模可控 |
| Fair Semaphore 性能开销 | 相比非公平 Semaphore 多一次 FIFO 队列操作，在虚拟线程场景下开销可忽略 |
| `Integer.MAX_VALUE` 默认值的 acquire/release 开销 | Semaphore 在 permits 充足时 acquire 仅一次 CAS，开销极小；如需极致性能可后续加条件跳过 |
| Carrier 线程 pinning 跨组影响 | JVM 层面限制，等待 JDK 24 JEP 491 修复；当前方案在逻辑层面隔离影响范围 |

---

## 12. 全面优化（Bug 修复 + 功能增强 + 测试补充）

### 12.1 Bug 修复与健壮性加固

| 修复项 | 文件 | 描述 |
|--------|------|------|
| Builder 防御性拷贝 | `GroupPolicy.java` | `perGroupMaxConcurrency()` 和 `perGroupMaxInFlight()` setter 立即拷贝传入的 Map，防止调用方后续修改泄漏 |
| Shutdown 语义统一 | `GroupExecutorManager.java` | `close()` 改用 `shutdownNow()` + `clear()`，避免任务卡住时无限阻塞 |
| Shutdown 清理缓存 | `GroupSemaphoreManager`, `GroupBulkheadManager`, `GroupExecutor` | 新增 `clear()` 方法，`shutdown()` 时清理所有 Manager 的 ConcurrentHashMap，允许 GC |
| shutdownGroup 竞态修复 | `GroupExecutorManager.java` | `shutdownGroup()` 使用 `ConcurrentHashMap.compute()` 确保 remove + shutdownNow 的原子性 |
| Semaphore 公平性统一 | `GroupSemaphoreManager.java` | Layer 3 也使用 `fair=true` 的 Semaphore，与 Layer 1/2 保持一致，避免饥饿 |

### 12.2 功能增强

#### TaskHandle 超时等待

`TaskHandle` 新增 `await(long timeout, TimeUnit unit)` 和 `join(long timeout, TimeUnit unit)`。超时返回 `CANCELLED` 状态的 `GroupResult`（error 为 `TimeoutException`），不自动取消底层任务。

#### CompletableFuture 桥接

`TaskHandle.toCompletableFuture()` 通过一个虚拟线程桥接 `Future` 到 `CompletableFuture<GroupResult<T>>`，支持链式异步组合。

#### 带超时的优雅关闭

`GroupExecutor.shutdown(Duration timeout)`：先 `shutdown()` 等待指定时长，超时后 `shutdownNow()` 强制终止。返回 `boolean` 表示是否在超时前全部完成。`GroupExecutorManager.awaitTermination(Duration)` 提供底层实现。

#### 分组缓存淘汰

`GroupExecutor.evictGroup(String groupKey)`：移除指定 group 的所有缓存资源（Executor、Semaphore、Bulkhead），下次该 group 有任务时重新创建。三个 Manager 均新增 `evict(groupKey)` 方法。

#### 任务生命周期监听器

新增 `TaskLifecycleListener` 接口：

```java
public interface TaskLifecycleListener {
    default void onSubmitted(String groupKey, String taskId) {}
    default void onStarted(String groupKey, String taskId) {}
    default void onCompleted(String groupKey, String taskId, GroupResult<?> result) {}
}
```

通过 `GroupPolicy.Builder.taskLifecycleListener()` 配置。`GroupExecutor` 在 submit/executeWithIsolation/executeTask 中调用，异常静默捕获。

### 12.3 测试覆盖

总计 42 个测试（GroupExecutorTest 28 个 + QueueThresholdTest 14 个）：

| 测试方法 | 验证内容 |
|---------|---------|
| `testSubmitAfterShutdownThrows` | shutdown 后 submit 抛出 IllegalStateException |
| `testExecuteAllAfterShutdownThrows` | shutdown 后 executeAll 抛出 IllegalStateException |
| `testMultipleCloseIsIdempotent` | 多次 close() 不抛异常 |
| `testCancelInterruptsBlockedTask` | cancel(true) 中断阻塞任务，返回 CANCELLED |
| `testResolverReturningZeroCoercedToOne` | resolver 返回 0 被修正为 1 |
| `testResolverReturningNegativeCoercedToOne` | resolver 返回负数被修正为 1 |
| `testResolverThrowingFallsBackToDefault` | resolver 抛异常回退到 default |
| `testGroupTaskNullValidation` | GroupTask 构造传 null 抛 NPE |
| `testBuilderDefensiveCopy` | Builder 防御性拷贝验证 |
| `testHighConcurrencyStress` | 10 组 × 100 任务压力测试 |
| `testTaskHandleAwaitWithTimeout` | await 超时返回 CANCELLED + TimeoutException |
| `testTaskHandleJoinWithTimeout` | join 超时返回 CANCELLED + TimeoutException |
| `testToCompletableFuture` | CompletableFuture 桥接正确完成 |
| `testEvictGroup` | evictGroup 后可重新创建资源 |
| `testTaskLifecycleListener` | 监听器回调正确触发 |
| `testTaskLifecycleListenerExceptionIsSilent` | 监听器异常静默捕获，不影响任务 |
| `testGracefulShutdownWithTimeout` | 带超时的优雅关闭 |

### 12.4 更新后的包结构

```
io.github.kobe
├── GroupExecutor.java             // 核心执行器（入口）
├── GroupPolicy.java               // 并发策略 + 隔离策略（Builder）
├── GroupTask.java                 // 任务定义（record）
├── GroupResult.java               // 执行结果（record）
├── TaskHandle.java                // 提交句柄（含超时 + CompletableFuture）
├── TaskStatus.java                // 状态枚举
├── TaskLifecycleListener.java     // 任务生命周期监听器接口
├── RejectionPolicy.java           // 内置拒绝策略枚举
├── RejectionHandler.java          // 自定义拒绝处理器接口
├── RejectedTaskException.java     // 队列阈值超限异常
└── internal/
    ├── GroupSemaphoreManager.java  // Layer 3: per-group 并发 Semaphore 管理（fair）
    ├── GroupBulkheadManager.java   // Layer 2: per-group 在途 Semaphore 管理（fair）
    ├── GroupExecutorManager.java   // Per-group 虚拟线程执行器管理
    └── GroupQueueManager.java      // Per-group 队列阈值 Semaphore 管理
```

---

## 13. Code Review 修复（并发安全加固 + 语义修正）

基于全面 Code Review 发现并修复的问题：

### 13.1 并发安全修复

| 修复项 | 文件 | 描述 |
|--------|------|------|
| evictGroup 原子性 | `GroupExecutor.java` | 引入 per-group `ReentrantReadWriteLock`：`submit()` 和 `executeWithIsolation()` 持读锁查找资源，`evictGroup()` 持写锁执行驱逐，防止混合新旧资源导致 permit 泄漏 |
| CALLER_RUNS 释放 permits | `GroupExecutor.java` | 在调用 `handleRejection()` 之前释放所有已持有的 global/bulkhead permits，使 CALLER_RUNS 不再占用隔离资源 |
| Layer 2 队列阈值保护 | `GroupExecutor.java` | 对 bulkhead acquire 增加 try-then-check-threshold-then-block 模式（与 Layer 1/3 一致），防止任务在 Layer 2 无限堆积 |

### 13.2 语义修正

| 修复项 | 文件 | 描述 |
|--------|------|------|
| startTimeNanos 语义 | `GroupExecutor.java` | 将 `startNanos` 捕获移至所有 permits 获取之后，`durationNanos()` 现在反映纯执行时间（不含排队等待时间） |
| Builder 参数验证 | `GroupPolicy.java` | `build()` 方法对无效参数（concurrency < 1、inFlight < 1、queueThreshold < 0）抛出 `IllegalArgumentException`，尽早暴露配置错误。动态 resolver 返回值仍通过 sanitize 纠正 |
| rejectionHandler 优先级文档 | `GroupPolicy.java` | 在 Javadoc 中明确说明：当同时设置 handler 和 policy 时，handler 优先，policy 被忽略 |
| awaitTermination 契约文档 | `GroupExecutorManager.java` | 添加 Javadoc 说明调用者必须先设置 closed 标志 |

### 13.3 新增测试

| 测试方法 | 验证内容 |
|---------|---------|
| `testBuilderRejectsInvalidValues` | Builder 对 9 种无效参数抛出 `IllegalArgumentException` |
| `testBulkheadWaitRespectsQueueThreshold` | Layer 2 bulkhead 等待受 queue threshold 保护 |
| `testCallerRunsDoesNotHoldPermits` | CALLER_RUNS 执行时不持有 global/bulkhead permits |

### 13.4 文件变更汇总

| 文件 | 变更行数 | 描述 |
|------|---------|------|
| `GroupExecutor.java` | +140/-17 | 新增 per-group ReadWriteLock、Layer 2 queue threshold、permit 释放逻辑 |
| `GroupPolicy.java` | +41 | Builder 参数验证 + rejectionHandler 文档 |
| `GroupExecutorManager.java` | +3 | awaitTermination 契约文档 |
| `GroupExecutorTest.java` | +97 | 3 个新测试用例 |
