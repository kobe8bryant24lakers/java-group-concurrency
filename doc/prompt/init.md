你是 JDK21 并发类库作者。请在当前目录初始化一个 Maven 纯 Java 开源类库工程，包名固定为 io.github.kobe（所有对外 API 必须在该包或其子包下），主题是“Java 分组并发（grouped concurrency），组内支持并发且并发度可动态计算”。

【工程与构建要求】
1) 生成标准 Maven 目录结构：
    - src/main/java
    - src/test/java
    - src/main/resources
2) 生成 pom.xml：
    - groupId: io.github.kobe
    - artifactId: group-concurrency
    - version: 0.1.0-SNAPSHOT
    - maven.compiler.release=21
    - 仅允许测试依赖 JUnit5（org.junit.jupiter:junit-jupiter）
    - 配置 surefire 支持 JUnit5
3) 不引入任何第三方运行时依赖（只用 JDK21 标准库）

【库功能要求（分组并发，组内并发可控 + 动态计算）】
4) 核心语义：按 groupKey 分组；组间并行；组内也并行，但组内最大并发度可控（1..N）
5) 实现方式：使用 JDK21 Virtual Threads（Executors.newVirtualThreadPerTaskExecutor）
6) 组内限流：每个 groupKey 对应一个 Semaphore(maxConcurrency)，任务执行前 acquire，结束后 release
7) 对外 API（至少实现这些）：
    - GroupExecutor.newVirtualThreadExecutor(GroupPolicy policy)
    - <T> TaskHandle<T> submit(String groupKey, String taskId, Callable<T> task)
    - <T> List<GroupResult<T>> executeAll(List<GroupTask<T>> tasks)  // 收集全部结果，不 fail-fast
    - void shutdown() / close()（实现 AutoCloseable 更好）

【动态并发度：GroupPolicy 设计要求（重点）】
8) GroupPolicy 必须支持 3 种并发度来源，并定义优先级（从高到低）：
   (1) perGroupMaxConcurrency 覆盖（Map<String,Integer>）
   (2) concurrencyResolver 动态计算（ToIntFunction<String> 或 Function<String,Integer>）
   (3) defaultMaxConcurrencyPerGroup 默认值（默认 1）
9) 规则与健壮性：
    - 任意来源得到的并发度必须 >= 1，否则按“受控方式”处理：
        * 建议：将非法值纠正为 1，并在 javadoc 说明；或抛出 IllegalArgumentException（但要统一策略）
    - resolver 可能抛异常：必须捕获并降级到 default 值（避免整个批次崩溃）
    - 为避免内存膨胀：Semaphore 缓存结构应支持按需创建；并提供（可选）清理策略或说明（例如：库不自动清理，适用于 groupKey 规模可控场景）
10) 并发闸门实现要求：
- Semaphore 需要按 “当前解析出的并发度” 创建
- 如果同一 groupKey 的并发度动态变化：
    * 策略要求：采用“首次解析固定”策略（推荐，简单稳定），即第一次见到 groupKey 时解析并创建 Semaphore，以后不再改变
    * 在 README 说明该策略（避免歧义）
11) 结果模型 GroupResult<T>：
- groupKey、taskId
- status（SUCCESS/FAILED/CANCELLED）
- value（成功） / error（失败）
- startTimeNanos / endTimeNanos / duration
12) 线程中断语义正确：
- 如果在 acquire 或执行中被 interrupt：恢复中断标记 Thread.currentThread().interrupt()
- 该任务返回 CANCELLED（或抛出受控异常并在 Result 里标记 CANCELLED），但必须统一策略
13) 对外类/方法写基础 javadoc；internal 实现放到 io.github.kobe.internal 包

【测试要求（JUnit5，必须可稳定通过）】
14) 写 4 个测试（新增动态并发测试）：
- testMaxConcurrencyPerGroup(): 同组并发度=2（通过 perGroup 覆盖），提交 8 个任务，断言观测到的“同组最大并发”<=2
- testParallelAcrossGroups(): 组A并发=2、组B并发=2，同时提交任务，断言总体并发峰值 >2（证明组间并行）
- testCollectFailures(): 混合成功/失败任务，executeAll 返回的 GroupResult 能正确标记 FAILED 并携带异常
- testDynamicConcurrencyResolver(): 不设置 perGroup 覆盖，使用 concurrencyResolver：
    * 例如：key 以 "vip:" 开头 -> 4，否则 -> 1
    * 提交 vip:1 的多个任务，断言其峰值并发<=4 且明显高于普通组（可断言 vip 组峰值>1）
      （提示：用 AtomicInteger current/max + CountDownLatch 同步起跑 + sleep 拉开时间窗口，避免偶现）

【文档】
15) 生成 README.md：
- 项目简介（分组并发、组内并发可控、动态并发 resolver）
- 快速开始（Maven 坐标、示例代码：resolver 示例 + perGroup 覆盖示例）
- 设计说明（Virtual Threads + Semaphore per group）
- 动态并发策略说明：首次解析固定 / resolver 异常降级 / 非法并发度处理规则
- 线程安全/取消语义说明

【最后输出】
16) 输出：
- 文件路径清单
- 每个文件的完整代码（可直接落盘）
- 本地验证命令：mvn test、mvn -q package