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
├── shutdown()
└── close()  [AutoCloseable]
```

**关键实现细节：**

- 私有构造器 + 静态工厂方法，确保只能通过 `newVirtualThreadExecutor` 创建
- 内部持有 `ExecutorService`（virtual-thread-per-task）、`GroupSemaphoreManager`、`GroupPolicy`
- `AtomicBoolean closed` 保证 shutdown 的幂等性和线程安全
- `submit()` 将任务包装为 `executeWithSemaphore()`，投递到虚拟线程执行
- `executeAll()` 先批量提交所有任务，再逐一 `await()` 收集结果，**不 fail-fast**

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
- 所有来源得到的并发度通过 `sanitize()` 处理：`raw >= 1 ? raw : 1`，非法值纠正为 1
- `concurrencyResolver` 抛出异常时静默捕获，降级到默认值，避免整个批次崩溃
- 构造时对 `perGroupMaxConcurrency` 做防御性拷贝 + `unmodifiableMap`

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
└── static cancelled(...)
```

- `status` 为 `SUCCESS` 时 `value` 有值、`error` 为 null
- `status` 为 `FAILED`/`CANCELLED` 时 `value` 为 null、`error` 记录原因
- 时间戳使用 `System.nanoTime()`，支持精确耗时计算

#### TaskHandle\<T\>

已提交任务的句柄，包装 `Future<GroupResult<T>>`。

```
TaskHandle<T>
├── groupKey() → String
├── taskId() → String
├── await() → GroupResult<T>       [throws InterruptedException]
├── join() → GroupResult<T>        [不抛受检异常]
├── cancel(mayInterrupt) → boolean
└── isDone() → boolean
```

- `await()`：等待完成，`CancellationException` 转为 CANCELLED 结果，`ExecutionException` 解包
- `join()`：await 的无受检异常版本，中断时返回 CANCELLED 结果并恢复中断标记

#### TaskStatus

```
enum TaskStatus { SUCCESS, FAILED, CANCELLED }
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
    executeWithSemaphore(groupKey, taskId, callable)
        │
        ├── 1. semaphoreManager.semaphoreFor(groupKey)
        │       → 获取（或惰性创建）该组的 Semaphore
        ├── 2. semaphore.acquire()
        │       → 获取许可（可能阻塞等待同组其他任务完成）
        ├── 3. executeTask(groupKey, taskId, callable, startNanos)
        │       ├── 成功 → GroupResult.success(...)
        │       ├── InterruptedException → 恢复中断 + GroupResult.cancelled(...)
        │       ├── CancellationException → GroupResult.cancelled(...)
        │       └── 其他异常 → GroupResult.failed(...)
        └── 4. finally: semaphore.release()
                → 归还许可（仅在 acquire 成功的情况下）
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
| `GroupSemaphoreManager.semaphoreByGroup` | `ConcurrentHashMap.computeIfAbsent` | 惰性创建保证每个 key 只创建一次 |
| 各组的 `Semaphore` | Semaphore 自身线程安全 | acquire/release 原子操作 |
| `GroupPolicy` 字段 | 不可变（构造后只读） | Map 为 unmodifiableMap |
| `GroupTask`, `GroupResult` | Java record（不可变） | 天然线程安全 |

## 7. 中断与取消语义

```
中断发生点              │  处理方式
────────────────────────┼──────────────────────────────────
semaphore.acquire()     │  捕获 InterruptedException
                        │  → 恢复中断标记
                        │  → 返回 GroupResult.cancelled()
                        │  → 不执行 release（未获取许可）
────────────────────────┼──────────────────────────────────
task.call() 内部        │  捕获 InterruptedException
                        │  → 恢复中断标记
                        │  → 返回 GroupResult.cancelled()
                        │  → finally 中 release（已获取许可）
────────────────────────┼──────────────────────────────────
TaskHandle.cancel(true) │  委托 Future.cancel(true)
                        │  → 中断底层虚拟线程
                        │  → 触发上述路径之一
────────────────────────┼──────────────────────────────────
executeAll 调用线程中断  │  当前 await 抛 InterruptedException
                        │  → 恢复中断标记
                        │  → 当前 + 剩余任务标记 CANCELLED
                        │  → 剩余 handle.cancel(true)
```

## 8. 测试策略

4 个核心测试用例，覆盖主要功能维度：

| 测试方法 | 验证目标 | 实现手段 |
|---------|---------|---------|
| `testMaxConcurrencyPerGroup` | 同组并发度上限（perGroup 覆盖 = 2） | AtomicInteger 跟踪峰值 + CountDownLatch 同步起跑 + sleep 拉开窗口 |
| `testParallelAcrossGroups` | 不同组之间真正并行 | 全局 AtomicInteger 峰值 > 单组上限 |
| `testCollectFailures` | executeAll 不 fail-fast + 失败结果正确标记 | 混合成功/失败任务，检查所有结果状态 |
| `testDynamicConcurrencyResolver` | resolver 动态计算并发度 | vip 组（resolver→4）vs 普通组（default→1），分别验证峰值 |

## 9. 包结构

```
io.github.kobe
├── GroupExecutor.java           // 核心执行器（入口）
├── GroupPolicy.java             // 并发策略（Builder）
├── GroupTask.java               // 任务定义（record）
├── GroupResult.java             // 执行结果（record）
├── TaskHandle.java              // 提交句柄
├── TaskStatus.java              // 状态枚举
└── internal/
    └── GroupSemaphoreManager.java  // Semaphore 缓存管理

io.github.kobe (test)
└── GroupExecutorTest.java       // 4 个核心测试
```

## 10. 设计约束与权衡

| 决策 | 选择 | 理由 |
|------|------|------|
| Semaphore 缓存策略 | 不自动清理 | 库假设 groupKey 规模可控；自动清理引入复杂性且可能导致并发度重新计算 |
| 并发度变更策略 | 首次解析固定 | 简单稳定，避免运行中 Semaphore 许可数动态调整带来的复杂性和不可预测性 |
| resolver 异常处理 | 静默降级到默认值 | 保证单个 resolver 异常不影响整个执行批次 |
| 非法并发度处理 | 纠正为 1 | 比抛异常更宽容，保证系统始终可用 |
| 执行器类型 | virtual-thread-per-task | 每个任务一个虚拟线程，天然匹配高并发场景，无需调参 |
| fail-fast 策略 | 不 fail-fast | executeAll 收集全部结果，调用方可自行决定如何处理失败 |
