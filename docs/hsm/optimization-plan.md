# HSM 性能优化方案

状态：继续实施，**尚未完成全部优化，也未开始本轮原版对比**。P1 普通日志自身回执、originating term 约束和单 RPC 普通 open 已实现；P2 已将 `mkdir`、创建/覆盖、块分配、flush/close、可写 open、非递归 SetAttr 和非递归 Delete 接入真实 committed 命令与 SDK 会话去重，rename/替换/交换、递归 SetAttr/Delete 和无 UFS 的 TTL Delete 已接入紧凑 committed 命令；少数其他 RPC 仍走过渡微批。owned 路径共享投影资格，相邻 prepared Native/HSM 条目可共用 proposal；各类请求共用 ReadIndex 资格队列，投影资格已增加 Recall/前台/后台加权调度；读取资格已改为 Raft ReadIndex，等待本地 apply 而不追加空日志。P3 读取保护批处理、基准记录/高频访问状态拆分及在线读增量原子写批已经实现；P4 已实现后台快照筛查、版本/当前路径复核、小批入队及共享预算汇总；P5 已实现范围清理、读结果有界保留和二进制 Worker 通道/连接复用。其余边界及验收状态见下表。

## 当前实现与完成门槛（2026-10-22）

| 项目 | 本轮代码行为 | 仍需完成或验收 |
|---|---|---|
| 逻辑元数据查询 | Exists、FileStatus、目录列表、块信息、锁/挂载信息使用有界已提交读取资格；最多 64 个查询共用 ReadIndex，128 个接收名额、512 KiB 请求/批次限制；不生成工作流结果或空日志 | 当前查询完成前仍保护旧式投影；大型目录响应与统一混合队列仍待优化 |
| 普通 open | `open_path` 在关闭 HSM 时直接返回普通块布局；不缓存“关闭”判断，避免换主后使用过期能力 | 全部优化结束后统一基准 |
| 原生写入链路 | SDK 使用 `NativeMutation` 稳定身份；mkdir/create/overwrite/add-block/flush/close/write-open/Resize/AssignWorker/非递归 SetAttr/Delete 及三个公开批量写 API，最多 32 条 owned 命令一次 Native journal 提交；Leader/Follower 同样条件应用、保存并返回原始响应；关闭 HSM 的卷一 RPC 执行普通路径 | 其余原生 RPC 及混合排序、去掉过渡 gate；当前 HSM 卷的此入口支持无 UFS namespace，UFS 挂载内返回持久失败，不跳过 UFS 同步后伪装成功 |
| 非递归属性 | 只检查和修改目标 inode，不遍历/刷新后代；文件属性与稳定 HSM 记录、候选索引、ctime、原始结果同批发布；最多 32 条 Native 命令合批 | 递归属性使用紧凑 committed 输入和整批原子发布；大子树内存/时延仍需验收 |
| 读取保护 | 最多 32 个条件读命令合并一次日志；普通在线读增量再合并为一次 RocksDB WriteBatch；批内读者/版本按顺序合并；原生 atime 每 inode 只写最终值 | 混合原生/HSM 统一提交；真实故障注入 |
| 大文件读结果 | 超过 32 块时，持久化原始授权、原始 FileStatus 和逻辑布局摘要；即时回复仍返回完整块位置，重试在有效保护下重建位置，不延长授权 | 大布局/复制位置变化及故障重试验收；此项减少结果编码量，不等同于吞吐收益 |
| 高频状态 | `record:{inode}` 保存稳定实体；`access:{inode}` 保存 base/version、revision、atime、readers；稳定变更原子合并并删除 overlay | 精确 atime 仍迁移候选索引、仍更新原生 inode，语义保持 |
| 租约 | 任期内单调时间；`read-clock:v1` 持久时间下界；新任期/恢复时对既有读者保守等待 120 秒；SDK 截止期从发请求时计算 | 节点暂停、墙钟跳变与换主的端到端组合故障测试；无日志 delegation 未启用 |
| 策略筛查 | 提交前缀后固定 RocksDB 快照，后台最多 4 个扫描；每页软预算 10 ms/2 MiB（允许首条进展），最多 256 行；不填充 RocksDB block cache | 前台持续 I/O 下的百万 namespace 验收；已增加后台加权资格，仍需验收实际 P99 |
| 策略入队 | 每批最多 16 个候选；重新检查 policy/run、record version、当前路径、读保护和共享预算 | 已用无日志 ReadIndex 建立前后读取资格；P2 完成后缩短跨提交 gate |
| 空扫描页 | 手动纯查询不记工作流结果；周期页面只条件提交 cursor/pressure，通过 apply 回执返回，无永久 outcome/binding；真正入队继续持久保存结果 | 新旧日志/checkpoint 及取消/丢回执回归 |
| 容量预算 | `budget:online:v1` 与 Ready/Running/终态 reservation 同批增减；正常读取 O(1)；旧 checkpoint 缺项时有界分页重建 | 并发规则与换主压力测试；不是 Recall 硬容量预留 |
| 冷恢复 | HSM 卷用一个 WriteBatch 的 7 个 range tombstone 清除原生 inode/edge/block/location、HSM、mount、lock 投影，随后从 checkpoint/尾日志重建；保留无关 common 事实；restore 排空快照读租约 | 完整百万恢复验收；namespace 构建和日志重放仍随实际恢复内容增长 |
| 格式与分配 | `hsm_native_v2/v3/v4/v5/v6` 空 CF 标记访问增量、Native journal 及持久 GC 格式；单条 access/progress 也用版本封装；Native variant 只追加，不改变历史编号；持久推进 inode 分配上限 | 旧 checkpoint 未记录过的历史已删除最大 ID 无法凭空恢复；P2 全路径接入仍待完成 |
| 后台写入排序 | TTL/配额回收的 namespace 修改保留相同 gate；失联 Worker 清理按 1024 项分页，页间重新排队并逐页交接副本修复；原始任期/失联 token 检查防止旧任务删除新位置；UFS 缓存失效在页内确认后安排 Worker 删除；换主后 TTL 重新入桶 | 后台修改也迁移为 owned 命令；旧路径的 Worker 删除还需全部改为持久 outbox |
| 覆盖写清理 | inode、HSM generation、候选索引、每块 GC 事实及副本位置同批提交；Worker 删除由已提交事实驱动；实际删除确认后提交幂等 GC ack，原子移除事实与旧位置；分页游标持久化 | rename 替换、递归 delete 与无 UFS 的 TTL Delete 已使用同一持久退休事实；其他旧路径和真实故障注入、百万负载验收仍待完成 |
| 读历史 | 时间身份 + 持久 floor 拒绝过期请求，每次最多回收 8 对普通读结果/索引；工作流原始成功结果不清理 | 旧 checkpoint 中无历史索引的 legacy outcome 不会被猜测删除 |
| Worker 等待 | Allocate/Seal/Delete 网络等待释放协调 gate，返回后重新 ReadIndex 与 namespace 资格检查；GC 合并最新记录；控制者切换后可接续原逻辑步骤 | 真实 Raft Leader 故障和网络分区验收；保留旧 namespace 写入的过渡 gate |
| 数据通道 | 1 MiB 原始二进制 body、有界 JSON 头；连接按 Worker/config 复用，失败/取消丢弃；私有布局、Seal、摘要校验仍保留 | 本轮完整回归后再测传输收益；单副本 Recall HA 不在此优化中假装解决 |

统一提交层采用两入口（submit/read）的内部模块和 SDK 私有适配；准备确定的 owned 变更集、稳定请求身份和原始回复，Leader/Follower 同样 apply。未选用“把全部旧 handler 放入双树影子环境”的方案：它还需隔离 TTL、统计、Worker 删除、mount 和后台写入，正确性改造面更大。不能仅删除当前 gate 或把非确定 handler 原样移到 follower apply。

`master/committed/` 已为 `mkdir` 和不覆盖现有文件的 `CreateFile` 接通生产 RPC、版本化 Native journal、双角色 apply 与本地应用回执。SDK 为这两类调用保存稳定身份；存储只解码一次业务输入，准备阶段复用原有创建参数校验。准备只复制触及的 inode/edge，不复制全 namespace。会话使用时间身份、序号、ack 退休下界、1024 条重试窗口及 32 MiB 结果预算；每次清理最多 8 条 ack 结果、最多 4 个过期会话范围。退休时间下界精确到毫秒并在同一 WriteBatch 推进，避免时钟回退后复活刚过期身份。

SDK 在客户端内部维护会话、序号和连续 ack；并发完成存在空洞时不越过未知请求。只为新的独立调用轮换会话，重试中的请求不改身份。首次正向绑定 namespace 时，服务端先返回 incarnation 而不执行原操作；关闭 HSM 不写负能力缓存。窗口/字节满或提交未知时继续用原身份对账；预算不足最终返回不确定结果。绑定过的客户端不自动切换到另一个 namespace。

当前仍持有过渡 gate 等待 Native journal，防止未迁移的普通写入污染条件 apply；提交未知且无法建立前缀隔离时仍故障退出。不能据 mkdir 的局部切换宣称整个 P2 已完成。

目录祖先 rename 不依靠重写所有后代路径缓存。读状态的通用 apply 与稳定视图按 inode 反查当前路径，在线读批量 apply 直接更新 inode ID 对应的原生 atime。测试覆盖先 rename 再读取；当前活动 HSM 保护仍会阻止相应目录 rename，未改变该并发语义。

本轮追加验证：23 项 HSM 相关单元测试、11 项 committed 相关过滤测试及真实三 Master/一 Worker 测试通过；后者同时检查读命令/RocksDB 写批比例，覆盖 40 块文件、紧凑结果重试、目录 rename 后读取及关闭后拒绝旧授权。路径 patch 接入 hsm-rs 后，workspace 的 30 项功能测试和全部文档测试通过。百万测试仍保持 ignored，尚未重跑规模验收或原版性能比较。RocksDB 元数据投影自身关闭 WAL，WriteBatch 合并减少的是投影写入、重复处理及编码开销；同步持久确认来自 Raft 日志，不能把投影批次减少直接说成同步 WAL 次数下降。

Native mkdir 接入后的回归：Master 全量 225 项通过、2 项 ignored；SDK 会话 2 项通过；真实集群验证 32 个 mkdir 合批、首次绑定无业务效果、成功/失败原始结果重试、参数重绑定拒绝、ack 退休及后台 TTL 提交。完整 hsm-rs workspace 的 30 项功能测试与文档测试再次通过。协调器使用统一 panic 监督，避免后台 task 消失而队列永久保持 running；这一点不代替后续故障注入验收。

关闭 HSM 的普通卷返回明确的 `LegacyResult`，保留原有确认行为；已经绑定 incarnation 的请求不能在关闭卷上执行。无 HSM 单 Master 与启用 HSM 三 Master 的两个真实集群用例均通过。后续比较必须区分 legacy 确认与 committed 确认，不能只凭共用 RPC envelope 就视为等价。

新文件创建接入后的 Master 225 项单元测试和两个真实集群用例通过，新增覆盖 32 文件创建合批、创建成功后重命名仍返回原始结果、零长度完成纳管，以及后续真实块写入/透明读路径。覆盖写、重新打开和块分配仍待迁移，保留原有 gate 路径；完成写入的进展见下文。

写入完成（`CompleteFile`，含 flush）已接入 Native 批处理：文件 inode、块位置、HSM 权威记录、访问 overlay 物化、候选索引及原始请求结果同一次 RocksDB WriteBatch 发布。准备阶段只操作触及的 owned 数据，原生 inode 与 HSM version 同时检查，冲突时不发布两者。批内 flush→close 读取私有前序结果；按 inode 关闭允许打开之后路径已经变化。复用同一份 generation 构建规则，避免旧钩子与新提交路径各自维护规则。Native journal v2 增加 HSM 和 location 变更；冻结 v1 DTO 后显式转换回放，公共 SDK envelope 仍是 v1。新增回归覆盖双角色一致性、原始结果重试、HSM 版本冲突和旧 journal 回放。Master 全量 227 项通过、2 项 ignored；真实三 Master/一 Worker 与关闭 HSM 的两个集群用例通过，包含 32 个并发关闭的合批断言及真实块写入后的透明读。块分配、重新打开、覆盖写和其他 namespace 修改仍未完成迁移。


专项回归覆盖读状态物化、预算去重、快照规则变更、范围清理后的重放、恢复等待短快照、时钟跳变及空页不增长历史。真实 MiniCluster 已验证同 inode 32 读者合批与覆盖隔离；本轮代码使用路径 patch 接入 hsm-rs 后，workspace 的 30 项功能测试及全部文档测试通过，包含普通写入纳管、策略执行、并发自动 Recall、控制器资格切换和进程重启；百万测试保持 ignored，未把前一轮百万结果当成本轮验收。尚不能据此声明 P2 或全部性能优化完成。

HA 为 P0/P1 起的前置约束。分层接管、微批换主、读取保护、控制器/执行器恢复及数据冗余见 [HA 设计](ha-design.md)。当前控制器自动接管、归档后端与 Recall 私有布局的端到端 HA 缺口仍待实现。

## 1. 优化目标与责任

普通写入自动纳管、策略筛选、访问自动 Recall 的行为保持。Curvine 保存文件身份、驻留、保护、归档锚点、任务与条件发布的权威事实；外部 hsm-rs 编排 Archive/Release/Recall，data-mover 搬运，后端封存对象。提交批处理和文件读写保护由存储侧统一实现，外部核心不维护 namespace 镜像。

本次普通在线读写基准没有运行外部 controller 或 data-mover；首先优化存储侧和 SDK。每文件开销没有必要随实际归档吞吐一起承担。

| 顺序 | 优化项 | 直接目标 | 实施风险 |
|---|---|---|---|
| P0 | 分段计时、相同确认语义及 HA 故障基线 | 分开测锁等待、日志排队、复制、apply；明确去重、换主与恢复资格 | 分项评估 |
| P1 | 普通日志自身的提交回执；关闭 HSM 时消除逐文件探测 | 移除普通修改额外的空 barrier；减少无效 RPC | 中 / 低 |
| P2 | 统一提交排序、微批与按序 apply | 同批容纳多个独立请求；等待 Raft 时不占用请求全局锁 | 高，核心改造 |
| P3 | 读取保护合批、拆分高频小记录 | 减少 open/close/renew 的提交和整份 Record 重写 | 中高 |
| P4 | 扫描快照、短事务入队、预算汇总及调度 | 降低后台争锁和每页重复提交 | 中高 |
| P5 | 恢复、结果保留和离线数据传输 | 控制长期历史及恢复成本；优化 Recall 后数据通道 | 分项实施 |

P1 本身不会消除全局串行上限。P2 中的批处理和缩短锁持有必须一起设计；持有当前全局锁等待每个请求提交时，其他请求无法充分进入同一个批次。

## 2. 先补齐可归因的测量

在请求进入、协调排队、取得 namespace 锁、写入 journal 队列、Raft propose、提交、local apply、唤醒客户端处记录耗时。补充实际 batch 条数/字节、每操作日志条数、proposal 与同步 WAL 次数、CAS 冲突/重试、Record 编解码字节、候选迁移、路径解析、扫描锁持有和队列积压。

除原版/关闭/开启 HSM 三组，再增加“原版普通日志也等待提交确认、但不增加 HSM 记录”的受控基准，区分强化确认语义的成本与 HSM 记录/协调的成本。各组确认语义在报告中显式标识；保留同步 WAL、既有副本数和提交安全性。

大文件写吞吐的 off 组差值仍需 profiling；已确认的 SDK open 探测不足以解释所有写入差值，不预先认定全部由某一个函数造成。

## 3. P1：直接等待本次普通操作的日志

本轮落地采用内存 FIFO 前缀提交标记：标记覆盖本次请求的全部原生日志，等待它们已有的 proposal/apply 确认，不追加持久化空 entry。每条入队日志携带运行时 originating term，本节点单次提交并由 Raft 节点检查 term。具体实现、取消和结果不确定语义见 [提交回执](commit-receipts.md)。下述 ticket 是原设计表达，当前没有为普通 RPC 新增持久化请求去重或 outcome 查询。

当前普通修改为：

```text
取得 hsm_mutation_gate
  → 修改原生 inode 与 HSM 投影，排入普通 journal
  → 再提交一个 payload 为空的 HsmEntry
  → 等待该 barrier 提交及本地 apply
  → 返回并释放 gate
```

改为给本次普通 journal 带提交 ticket：

```text
取得现有 gate
  → 执行普通修改并取得对应 journal ticket
  → 等待该操作自己的 committed/local-applied 回执
  → 返回并释放 gate
```

具体落点：

- [JournalWriter](../../curvine-master/src/master/journal/journal_writer.rs) 增加普通操作的 receipt；包含多个普通 journal 的单个请求，要等待完整操作的有序末尾 ticket，不能误把中间一步视为全部完成。
- [SenderTask](../../curvine-master/src/master/journal/sender_task.rs) 返回所提交批次的 applied index，并映射到 ticket。现有 Raft `ProposeResponse.applied_index` 已有基础，但还要确保当前节点的应用进度和请求所属 term/任期一致。
- [MasterHandler](../../curvine-master/src/master/master_handler.rs) 等待自身结果，移除这一路的额外空 HSM entry。初期保留现有 gate，先验证顺序等价。
- ticket 必须在入队前登记；超时/取消不表示操作失败或可以撤销，重试使用相同逻辑请求身份。提交状态不确定时保留可查询结果。

这一步减少日志与刷新轮次，单请求仍需真实提交等待；吞吐的主要释放依赖下一阶段。

SDK 同期增加显式的 HSM enablement/capability 协商。当前 [MasterHandshake](../../crates/client/curvine-client-core/src/file/handshake.rs) 可扩展，但静态“支持 HSM 协议”与实际 volume 启用状态要分开。缓存绑定 cluster/namespace incarnation、连接或 leader/config epoch，变化时重协商。仅在已确认关闭 HSM 的新卷上直接走普通 open；未知或缓存失效时重新确认。已经有 HSM 格式标记的卷继续禁止直接关闭 HSM。

## 4. P2：收敛到统一提交 Module

建议增加存储内部 `CommittedMetadata` Module，以小 Interface 隐藏批处理、顺序、幂等、恢复和回执：

```rust
// 设计伪代码，不是现有类型。
submit(command, request_identity) -> CommitReceipt
wait(receipt) -> CommittedResult
read_snapshot() -> CommittedSnapshot
query(request_identity) -> CommittedResultOrUnknown
```

Interface 的 receipt 绑定逻辑请求身份及提交任期/位置，重试保持 client session/sequence，不能只靠一个可能在换主后被重用或覆盖的日志位置判断成功。查询快照携带 applied index；需要线性一致的读取还须验证当前 leader 的读资格并等本地应用到相应位置，具体 ReadIndex/read-lease 实现收敛在 Module 内部。

普通元数据修改、HSM 状态转换和读取保护都进入同一个排序入口。队列收集一小批命令，以一次有序 journal/Raft 提交承载；各命令独立返回自己的结果。执行及结果发布都依据相同日志顺序，结果到达 committed/local-applied 后才向调用者确认。

需要一起解决现有“普通路径先修改；HSM 路径在 committed apply 修改”的差异：[JournalLoader](../../curvine-master/src/master/journal/journal_loader.rs) 当前 leader 对普通 entry 和 HSM entry 的处理不同。不能删除 gate 后继续让 HSM 在同一份投影上读取未提交的普通变更，也不能把两种现有 apply 路径直接混入一个无条件 batch。

目标采用 committed 状态进行条件执行和发布；将需要的时间戳、分配标识、Worker 选择等非确定输入记入命令。Leader/follower 对新命令执行相同状态机，去重与 per-command 结果在同一提交序列中确定。批内前一命令造成的状态变化对后一命令可见，条件在依次 apply 时再判断，不能仅在所有命令入队前预检一次。

迁移的第一步已实现为“协调器持有一次 gate，接收一批普通请求，统一等待本批提交”；HSM 请求仍在批次之间串行处理，见 [过渡微批](mutation-microbatch.md)。它保留现有投影保护，不能宣称已经实现混合命令并行。最终切换新命令格式需要版本激活点，并验证旧 journal/checkpoint 的恢复；不借机重解释历史日志。

### 批处理与锁的具体约束

- 首次试验可用 0.5—1 ms 最大集批等待、32—128 条及约 256 KiB 上限；都是调参起点，最终由 P99、实际 batch size 和 WAL 次数决定。只有请求条数上限没有字节/排队上限仍可能造成尾延迟。
- 修改 [SenderTask](../../curvine-master/src/master/journal/sender_task.rs) 中遇到每个 `HsmEntry` 都前刷/强刷的行为；采用有界批次和按命令回执。这个改动依赖统一顺序与条件执行，不能先单独去掉 force flush。
- namespace/global 锁仅用于短时间解析、变更或发布。等待 Raft、Worker Seal、归档复制、摘要计算时不持有该锁。
- 同 inode 的 open/修改/Release/Recall 按日志顺序处理；不同 inode 的准备、查询和数据搬运可并发。Raft apply 保持有序，不宣称并行状态机执行。
- rename/link/unlink 等涉及父目录或多个 inode 的操作采用固定顺序取得所需锁，重校验路径与名字。不能简单把全局锁替换成一个 inode 锁。
- 单个条件失败返回该命令的 Conflict/Fenced，不伪装成全批成功；底层持久化或确定性 apply 故障按现有安全原则处理。
- 队列按条数、字节和等待时间限流；优先服务前台和访问 Recall，同时为扫描/GC 保留最低进度，避免后台饥饿。大数据搬运不在这条队列里执行。

## 5. P3：降低读保护的固定开销

### 先保持现有租约语义，减少提交次数

`read_path`、`read_close`、`read_renew` 都作为可重试的条件命令进入统一队列。权威检查、注册/更新租约、更新访问观察和读取布局在同一状态机步骤完成；返回块位置或恢复布局前，确保该读取保护已生效。无需每个命令先提交一个空 HSM barrier 再提交一次状态更新。

同一 SDK 进程可合批 close/renew，存储也跨请求合批；初期显式 complete 仍在相应操作确认后返回，open/renew 仍保持安全确认。不能为了减少写入直接跳过访问观察，否则旧 Release 观察可能继续成立。

当前实现的单次 open 和 close 各包含前置 barrier 与状态提交；优化目标是各用一个实际条件命令，并与其他请求共用提交批次。不能承诺完全去掉首次读取的提交成本。

### 拆分 Record 的高频字段

目前 [Record](../../crates/common/curvine-model/src/hsm/native.rs) 把 anchors、布局、views、readers、atime 和状态放在一个 JSON。建议在同一 Curvine 权威域内按更新频率分开保存：

| 记录 | 典型内容 | 更新时机 |
|---|---|---|
| 稳定实体状态 | DataKey、generation、residency、长度、摘要、锚点、布局 | 写入完成、Archive、Release、Recall |
| 小型观察/修订记录 | 公开 revision、atime、访问观察及 CAS 依据 | 每次有效访问或状态变化 |
| 读取租约 | inode/session、generation、epoch、单调序号、deadline | open/renew/close |
| 候选与 outbox | 既有排序键、seal/GC 事实 | 仅派生结果变化时 |

一次命令涉及的字段、修订、索引与保护仍经同一 Raft/WriteBatch 更新；合成状态查询使用一致快照。保持公开 EntityState.revision 和旧观察失效语义，不把 access 拆出去后漏掉 Release 的校验。这里没有引入外部数据库或 namespace 镜像。

精确 atime 变化仍迁移精确时间索引。时间桶或合并 atime 只有在另行明确时间精度、候选排序和访问使旧 Release 失效的机制后才评估。

### 更激进的读租约复用，单独作为后续设计

可评估按 inode/客户端会话授予可复用的短期读取 delegation，多次热文件打开共享一次已提交授权。Release 先进入可恢复的 draining/revoking 阶段，阻止新授权，完成撤销或等待授权到期后再移除布局。换主、旧客户端恢复、时钟与超时、续期及 Worker fencing 必须一起设计；普通 Worker 块读取也需要支持相应验证，不能只在 Master 内存里记一个引用计数。

该方案减少热文件逐次协调，但可能推迟 Release；它是后续协议优化，前几步不依赖它。当前基准中的 Native Online 读取也不会因此被宣称可直接无保护打开。

## 6. P4：筛选与入队分离

目前 `policy_scan` 经全局 HSM gate 执行，包含 Record 查询、路径解析、进度与入队提交。建议拆成两段：

1. 从已提交快照分页做筛查，不占用跨 Raft 等待的全局请求锁。先用元数据拒绝，路径仅按需解析；给扫描设置时间/字节预算，而非只有 256 项上限。在线繁忙时从 32/64 项开始自适应，记录首项发现延迟与完整扫描耗时。
2. 将少量候选交给短条件命令，重新校验 policy revision、身份/generation、访问观察、当前驻留、pin/readers、当前路径归属、active intent 及共享预算，然后原子入队。

目录祖先 rename 未必改变每个后代 Record.version，不能只重验 Record 就接受过时路径。入队 apply 必须重新解析候选当前路径，或校验足够的目录版本。扫描快照也不能仅靠 ReadIndex 读取仍含 speculative 普通变更的投影；它依赖前述 committed 状态。

零匹配页面不必都制造完整工作流 outcome；将纯查询与真正修改拆开。持久扫描游标可以分批检查点，中断后重扫由入队幂等保证；规则版本、压力状态或预算有变化时仍须提交。不会因为少持久化游标而允许重复动作。

当前 `outstanding_bytes()` 每次累加所有在途 reservation。可增加源端汇总计数，和 reservation 的建立/释放在同一条件提交中更新，重试不重复增减，并提供有界重建核对。该计数仍是预计回收预算，不升级为 Recall 的硬容量预留。

额外 path/owner 复合索引等后续按 profiling 选择；先解决争锁、提交和重复预算扫描，避免给所有文件增加无用索引。

## 7. P5：长期规模与恢复后的数据通道

- checkpoint 需要把原生 namespace、HSM 状态/索引/任务和 applied index 对齐，再重放尾部日志。先测量现有成功恢复路径的各阶段，不能为了启动快保留未经校验的旧投影。
- 常规读取保护的去重尽量利用 session/epoch/sequence，避免每次只为一次性 read-close 保存无限量随机 outcome。删除终态历史前先定义重试窗口及拒绝旧请求的持久机制；归档与 Release/Recall 的权威成功结果不能按时间随意清除。
- [hsm_worker_rpc](../../crates/client/curvine-client-core/src/file/hsm_io.rs) 目前每次建立连接，私有恢复布局以 64 KiB 分块、JSON 字节数组传输。后续可连接复用、二进制 framing、较大块及流水化，并评估恢复到普通块布局。稳定视图、能力校验、Seal、摘要校验和原 inode 条件发布保持。
- 上述私有通道不是本轮 Native Online 大文件测试的数据路径，不能用它解释已经测得的全部 32%/22% 差值。先依据分段计时定位，再改数据通道。

## 8. 实施与验收顺序

先定义并执行 [HA 验收矩阵](ha-design.md#9-ha-与性能一起验收)，将提交/去重、换主恢复和数据保护作为每阶段门槛；冷启动恢复不再作为最后才考虑的 HA 工作。建议拆成独立可回退的交付：P0 → P1 → P2 的普通请求微批 → P2 的统一 committed 路径 → P3 → P4 → P5。P1 的 SDK 关闭路径可以独立进行；Record 格式调整和 delegation 不与第一轮提交改造混在一起。

每一步先验收结构指标：额外 barrier 是否移除、同一批实际覆盖几个请求、每操作 WAL 次数、是否仍持锁等待网络、元数据编码量与扫描锁持有。然后重复原有三轮基准，增加并发 1/4/16/64、独立 inode 与单热 inode 两类，报告吞吐和 P50/P95/P99；大文件继续使用 4 GiB 写/12 GiB 读。

安全回归至少覆盖：

- read-open 与 Release 的两个先后顺序；保护建立前不能放行数据，保护存在时不能删除布局。
- 访问废弃旧 Release；rename/祖先 rename、覆盖/截断、generation 更新和候选重选。
- 批内同 inode 命令依赖；不同 inode 互不误 Fenced；一个 CAS 失败不污染其他结果。
- 批次提交前后强杀/换主、ACK 丢失重试、客户端取消、Worker Seal 后控制者重试；相同身份的动作不能重复生效。
- 旧 leader 不发布未提交或不匹配请求的结果；已提交原始结果可以跨任期查询返回。能力缓存失效；若启用 delegation，过期客户端与旧授权不能继续读已释放布局。
- GC 只处理已经提交的回收事实；并发读取合并一次 Recall，恢复仍发布至原 inode。
- 扫描快照到入队之间修改规则、目录、访问与共享预算；不能误选或超额重复记账。
- 新格式激活后的 checkpoint/旧日志恢复，百万 namespace 再验收，以及持续前台 I/O 下的周期扫描。

收益必须重新实测；本方案不保证通过几个配置项就恢复到原版的 5000+ 次/秒，也不把原版异步确认的延迟当作已提交操作应直接达到的延迟。


### 追加里程碑：块分配和重新打开

`AddBlock` 与可写、不截断的 `OpenFile` 已进入 Native 请求身份及提交链路。同批后续块请求可看到前序尚未发布的分配，首次分配后的重试不再次申请 Worker。准备/已知条件失败撤销本地调度计数；提交未知时保留保守计数，由原有 Worker 报告/超时收敛。块分配不为每块重写稳定 HSM 记录；完整关闭再原子纳管。首次重新打开推进 generation，后续并发写者保持同代次，集合成员的 CAS 摘要按逻辑值排序，跨进程重建不依赖 HashSet 遍历顺序。

验证：Master 229 项通过、2 项 ignored；两个真实集群用例通过，新增追加写、open 创建、块准备预算失败和同批分配去重。修复了回归期间发现的通用执行器启动/停止竞态：线程启动不再覆盖已发布 STOP，停止仍排空已接收任务；35 项 Runtime 测试通过，包含确定复现与清空队列回归。该修复独立提交。覆盖写与其持久清理、其他 namespace/background 修改、统一混合批处理仍待完成。


### 追加里程碑：覆盖写与持久清理

`CreateFile` 覆盖和可写 `OpenFile(O_TRUNC)` 复用 owned 覆盖准备。原 inode 保留，保留 `next_seq`；每次覆盖明确推进旧 generation，即使原文件仍处于 writing。新内容代次、长度、候选项、旧块清理事实及原始返回结果原子发布。普通不受管文件的清理同样不依赖 HSM Record 存在。

新增 `native-gc:v1:<block_hex16>` 与持久游标。首次退休为预分配但尚未上报的副本也保留位置哨兵，不能把“未上报”当作“已删除”。GC 重放可以重新下发 Worker 删除，清理确认使用 Native journal v3 的维护分支；仅匹配当前 incarnation/epoch 和原事实字节时移除，重复应用无副作用，不为每块制造永久结果。冻结 v1/v2 journal DTO，新增空 CF `hsm_native_v4` 阻止旧二进制误开。

验证：Master 232 项通过、2 项 ignored；两个真实集群用例通过，新增 40 旧块跨页清理、实际 Worker 删除确认、新布局可读、旧块 ID 不复用以及 GC 不增长永久 HSM outcome 字节。hsm-rs workspace 的 30 项功能测试及全部文档测试通过（路径 patch 指向本轮代码），包含进程重启、策略接管、自动 Recall 与旧 Release 失效。另单独修复旧 AddBlock 回放遗漏 next_seq 下界：确定回归从原先错误的 0 恢复为期望的 8，避免升级/崩溃后复用旧块。旧完整 checkpoint 未记录过的已删除 inode 最大 ID 仍不能凭空补出。


### 追加里程碑：退休副本与迟到上报

HSM 卷的块上报同时检查 inode 和当前布局成员，inode 仍存在不再意味着旧块仍属于在线布局。存在已提交退休事实时，上报会再次安排删除并保留位置哨兵；只在 Deleted 或完整报告对账证明缺失时消除相应位置。GC 完成后再出现的过时块报告不能重建该位置。

Release 的 `Record.native_garbage` 同批建立每块恢复索引及预分配哨兵，旧 checkpoint 没有索引时仍检查 Record 的清理义务。未变化的清理事实不重写索引或重新建立已确认的位置。GC 下发集合合并原退休清单与当前上报位置，覆盖准备后出现的新副本，避免只等确认却没有给新副本发删除。两个确定回归分别捕获旧布局重新登记和迟到副本未安排删除。

验证：Master 234 项通过、2 项 ignored；旧块报告校验后的两个真实集群用例通过；最新完整 HSM workspace 的 30 项功能测试及全部文档测试通过，包括五个真实集群场景。TTL standby 用例同时修正一个已有的时间边界假设：10ms 桶内创建与 close 可能落入两个桶，断言待机跳过应保存原队列，而非假定固定一条。


### 追加里程碑：外部 HSM 请求的执行生命周期

普通 HSM 控制与策略筛查增加 128 个有界接收名额；名额随存储执行任务保留至完成，调用方断开/取消不释放一个仍在执行的动作。接收前取消不生成后台任务。普通控制 JSON 只解码一次，panic 使用与 Native/Read 协调器相同的故障监督；提交未知时仍建立前缀隔离。读取保护保留自己的有界批处理入口。

验证：Master 235 项通过、2 项 ignored；两个真实集群用例通过，新增暂停协调 gate、取消控制者调用、恢复后 epoch 仍只推进一次，以及原请求重试返回原始结果。单元测试验证接收前取消不执行动作，接收后取消仍占名额直到动作完成。后续继续缩短 Worker 网络 I/O 的协调锁范围，当前尚未据此删除过渡 gate。


### Worker I/O 解耦的前置修复

Worker 现在能为尚未到达的 Allocate 持久保存 deleted-only 记录。Delete 确认前先保存退休状态和 owner，再 unlink/目录同步；相同 owner 可重复确认，错误 owner 及迟到 Allocate/Verify 均被拒绝，进程重启不解除。已有活对象 JSON 的 layout 编码保持，新增 null layout 只用于已退休且从未绑定布局的对象；旧解析器对此失败，不会将它当作未分配并重新创建。

确定回归先复现原实现的 NotFound，再验证重启后的迟到请求拒绝。Worker 全量 64 项通过、1 项 ignored。此项为将 Allocate/Seal/Delete 网络等待移出元数据协调边界提供前置保护，尚未把移出本身标为完成。


### 追加里程碑：Worker 网络等待不占用协调锁

Allocate、Seal 和私有布局 Delete 的 RPC 等待现在释放协调 gate。释放之前必须已有提交的分配预留或不可变清理事实；未确认的元数据提交不能跨越这个间隔。Worker 返回后重新取得 gate，执行 ReadIndex 并核对 storage/incarnation/controller epoch/root/format，只有重新取得资格才能继续准备或发布。资格丢失不会保存为永久业务失败，新控制者可用原 operation_id/attempt_epoch 和新 controller_epoch 接续。

GC 在 Delete 确认后重新读取最新 Record，只删除已确认的不可变 layout ID，保留等待期间更新的读者、锚点及其他清理义务。已缓存的 prepare 也只对同一次逻辑尝试、同一目标的 pending 分配发出 Allocate。协调 gate 仍保护旧式 namespace 修改，尚未删除。

验证：Master 235 项通过、2 项 ignored；两个真实集群用例通过。新增 TCP 代理暂停真实 Worker Seal 回复：原实现在此期间阻塞独立 mkdir，新实现允许 mkdir 完成；在同一暂停窗口提交控制者接管，旧控制者返回 Fenced，新控制者重试原逻辑步骤成功发布。此用例验证控制者切换，不等同于真实 Raft Leader 故障切换。针对 `977d179c` 的完整 hsm-rs workspace 回归通过：30 项功能测试（含五个 managed 集群场景和一个 native 集群场景）及全部文档测试通过；2 项规模/子进程入口保持 ignored。百万验收尚未执行。


### 追加里程碑：非递归属性修改

修正非递归目录属性操作仍检查整个子树的问题：后代的读取保护不影响不触及后代的 chmod，递归操作继续检查后代。非递归目录修改不刷新后代 HSM 记录，工作量与路径深度及目标元数据大小有关，不随后代文件数增长。回归保留一个活跃读者，验证非递归操作成功、递归操作拒绝；owned 准备只写目标 inode，不为后代建立观察。

SDK 的非递归 SetAttr 接入 Native 会话去重、合批和条件发布；文件属性、HSM 权威记录、候选索引及原始返回值同批提交。ctime 由 Master 捕获并统一盖戳，用户 xattr 不能伪造或删除；业务 mtime 的既有 generation 失效规则保持。递归 SetAttr 继续使用过渡入口，新 Native 入口明确拒绝未实现的递归命令。

验证范围：双角色一致应用、同批 owner→mtime 依赖、准备无可见副作用、返回原始结果重试、HSM version 冲突时 inode 不改变，以及真实集群 32 次非递归属性修改合批和关闭 HSM 卷的普通 SDK 行为。Master 237 项通过、2 项 ignored；两个真实集群用例通过，包括 32 次非递归属性修改的合批断言、控制者交接和关闭 HSM 卷的 SDK 属性修改。


### 追加里程碑：非递归删除与 inode 独立的物理清理

普通文件/链接和空目录的非递归 Delete 接入 Native 身份、条件检查与最多 32 条合批。目录项、inode 删除/链接数更新、HSM Record/访问 overlay/候选项清除、每块退休清单、预分配副本哨兵和原始删除计数在同一 WriteBatch 提交；随后由 `gc_batch` 根据已提交清理事实下发 Worker 删除和确认。清理事实不要求 inode 或 Record 继续存在；inode 已消失但仍有退休清单时，迟到块上报继续按 Retired 处理。

目录元数据 CAS 摘要不包含子目录项。空目录删除在 apply 额外检查真实 children，防止同一毫秒内新增文件、父目录元数据摘要恰好不变时误删。inode 分配上限不降低；请求重试返回原始结果，不删除同路径重新创建的文件。未纳管的硬链接保留其他目录项，最后一个链接才退休物理块。非空递归删除仍走过渡入口，Native 入口不伪装支持大子树原子删除。

Native journal v4 激活删除效果，保留 v3 字段编码并继续读取 v1/v2/v3 历史；公共 RPC envelope 仍为 v1。新增空 CF `hsm_native_v5` 使不支持新语义的二进制拒绝打开 checkpoint；需要所有 Master 支持该格式后使用，未实现跨版本滚动激活。

验证：Master 242 项通过、2 项 ignored；两个真实集群用例通过，含 32 次非递归删除合批、空目录删除、真实 Worker 在 inode 删除后的清理确认，以及此前的 Seal 等待/控制者接续回归。单元测试另外覆盖原结果重试、硬链接、双角色空目录删除、同时间戳新增子文件和 v3 日志回放；checkpoint 格式栅栏拒绝旧 opener。百万验收和本轮原版对比尚未开始。


### 已成功工作流之后的 GC 不确定结果

工作流的原始成功与后续清理提交分别对待。后续 GC 不确定时保留已提交的清理义务、不撤销 Release，但在释放协调 gate 前必须确认 journal 前缀；确认失败停止当前 Master。不能仅因外层回复已是成功，就忽略内部尚未确定的元数据提交。修正后的 Master 242 项及两个集群用例再次通过；该验证没有模拟丢弃 GC 提交回执，组合故障矩阵仍待完成。


### 累计集成验证（代码 a3822c28）

路径 patch 指向当前 Curvine 和本地相同 hsm-core 后，hsm-rs workspace 的 30 项功能测试（含五个 managed 集群场景及一个 native 集群场景）和全部文档测试通过；两个规模/子进程入口保持 ignored。包含普通写入纳管、Native SetAttr 后策略筛选、Release、并发自动 Recall、控制者资格变更与进程重启。Curvine 自身另有 242 项 Master 测试、两个优化集群用例通过。此记录没有执行百万验收、没有产生最终性能差值，也没有升级 PVE 集群。


### HA 回归发现并修正元数据查询旧前缀

新增普通文件删除后全进程重启、排除原 Leader 的测试，先复现 Exists 在新 Leader 当前任期提交/应用追齐前返回旧路径。通过 HSM inspect 的 ReadIndex 后查询即正确，确认是读取资格缺口，删除日志与清理事实并未丢失。

逻辑元数据 RPC 现进入 MetadataReads：每批取资格后才授予同步只读 handler；协调 gate 保持到该批查询完成，避免旧式 writer 的未提交投影。客户端取消关闭只读 ticket，不会留下未确认修改；不记录永久结果，不替代文件内容读取的租约。GetFilesystemInfo/心跳等派生容量状态继续原有路径。

验证：Master 242 项、两个真实集群用例及完整 HSM workspace 的 30 项功能/全部文档测试通过。集群用例检查 32 个元数据查询合批、不增加空 barrier 或工作流结果，以及取消后后续元数据操作可完成。增强的进程重启用例要求首次 Exists 自身正确，随后检查 Record 不存在、退休清单仍被处理并最终收到 Worker 删除确认；不借助预先执行 HSM 查询掩盖旧前缀。此场景是保留数据目录的全进程重启，并非单节点在线故障或磁盘丢失验收。


### 追加里程碑：owned 路径共享投影边界

过渡投影 gate 改为读写边界：Native owned 命令、读取保护批处理、纯元数据查询和策略快照取共享资格；旧式 handler/background 修改、普通 HSM 控制、策略入队及 namespace 初始化取独占资格。共享路径只准备 owned 变更，仍等待实际 commit/local apply，按 inode/HSM version 条件发布；同 inode 的并发变化可以被 CAS 拒绝，不把它描述成无冲突并发。旧式先修改投影的代码不能进入共享段。

Native 已绑定请求只读取现有 ordinary_format>=2 的 namespace。读取及扫描在进入共享段前，由 owned 协调任务完成必要的独占 namespace 创建/升级，不确定时在独占资格内确认前缀。Writer/reader/progress 的实际状态机顺序仍由 Raft 决定。此改造没有更改磁盘编码，旧式路径未迁移前仍需保留独占边界。

验证：Master 242 项、两个真实集群用例通过。新增共享资格持有期间 Native mkdir、数据读/close 和 FileStatus 均可完成，旧式 rename 等待共享资格释放后才执行；首次直接 open_path 能完成 namespace 初始化。完整组合 HSM 回归随相邻 prepared 日志合批一起执行。


### 追加里程碑：相邻 prepared Native/HSM 条目共用 proposal

发送线程只收集当前已经就绪的 Native/HSM entry，不增加固定等待定时器。每轮最多取 32 条，以 8 MiB 为 proposal 收集预算；单项可单独进展，后续项超预算时拆开提交。Native/HSM 的原始 ticket 仍分别对应自身本地 apply 结果，并共同等待该 proposal 的真实 committed/apply 回执。此处的 entry 是一个已经准备好的命令批，不能与每批内部最多 32 个逻辑命令混淆。

遇到 legacy entry、任期变化、提交 fence 或 snapshot 保留原顺序与边界；不将先修改投影的旧 handler 混入 owned 提案。不确定/缺少 applied index 时整批 ticket 返回未知并保留待处理批，运行线程停止当前 Master，不能重提 speculative 内容。`authority_proposals/authority_entries/mixed_authority_proposals` 用于记录实际结构指标。

验证：Master 247 项、两个真实集群用例通过。新增 transport 测试检查混合顺序、各自回执、legacy/term/fence 边界、取消回执、缺失提交证明及 32 条上限。真实集群同时执行独立 inode 上的 64 次创建和 64 次受保护读取，再关闭所有读者，验证结果及后续操作；不以单元测试证明实际工作负载必然出现混合 proposal，也不将合并能力直接称为实测吞吐收益。独立队列的调度、其余 namespace/background 修改仍待收敛。


### 累计组合回归（代码 535958d8）

共享投影资格、相邻 prepared entry 合并及元数据读取资格共同启用后，路径 patch 的完整 hsm-rs workspace 再次通过 30 项功能测试（包括五个 managed 场景和一个 native 场景）及全部文档测试。增强的全进程重启用例验证第一次 Exists 正确、已删除 Record 不复活、清理事实恢复与 Worker 最终确认。Curvine 自身另有 Master 247 项通过、2 项 ignored；两个优化集群用例通过。

这些是结构与正确性回归，仍未执行本轮百万验收或最终性能比较。后续重点是其余 namespace/后台路径迁移、队列级调度及故障矩阵；保留现有独占旧投影边界，不能据此宣称全部 P2 完成。


### 追加里程碑：目录命令、共享资格与后台删除

详见 [已提交的目录操作与后台删除](native-namespace.md)。rename/替换/交换、递归 SetAttr/Delete 改用紧凑 Native journal v5；只在 committed apply 展开子树，原始结果及 namespace/HSM/退休事实同批发布。读取保护时钟随命令记录，Follower 不使用自己的当前时钟重新决定是否存在有效读者。新增 `hsm_native_v6` checkpoint 栅栏，历史 v1–v4 解码不改。

HSM 卷无挂载时，TTL Delete 进入同一 Native FIFO；提交时核对 inode、元数据、过期条件、HSM 归属与目录实际为空。配额回收移到 blocking 线程，不再把 TTL 延期/跳过算作回收空间；失联 Worker 的正反位置索引每 1,024 块同批清理。各业务的读取资格共用 128 名额、64 等待者/轮的 ReadIndex FIFO，等待 I/O 时的新请求另取证明，不共享过去的证明。

验证：Master 257 项通过、2 项 ignored；两个真实集群用例通过，新增真实 SDK 目录改名、递归策略属性、rename 覆盖、活跃读取期间递归删除拒绝、关闭后删除、Worker 清理完成及 OS TTL 删除。单元回归另覆盖 2,048 文件、硬链接、捕获时钟、同元数据摘要下新增子项、候选 inode 被替换、资格取消/任期隔离及跨清理批次保留其他 Worker 副本。大子树展开仍为 O(N) 工作及写批内存；没有据此声称百万验收通过。

完整优先级业务调度、其余稀有 namespace RPC、UFS 旧式后台副作用以及组合故障矩阵仍待收敛；本轮百万验收与最终原版对比继续暂缓。


### 追加里程碑：递归删除规模路径与 Native Resize/AssignWorker

纯递归删除在内存树仅摘除待删子树的边界，不逐个重建父路径或对所有移除项排序；每个待删目录只范围检查自己的目标目录项，避免目录数乘删除项数的全量扫描。新增 128 目录/512 文件回归，保留原始结果重试和同名重建保护。

Resize/AssignWorker 接入 Native owned 命令；操作时间、布局与 Worker 选择在准备阶段确定，Follower 只应用效果。截断的整块持久退休与新逻辑布局、HSM generation、原始结果同批写入。未分配稀疏块保留 alloc_opts/空位置；重复已分配请求不重新预留 Worker，原身份重试在后来截断后仍返回原分配结果，但不能把旧块重新挂回文件。

最新 Curvine 回归：Master **259 通过、2 ignored**，两个真实集群用例通过。实际 SDK 验证 13,000 字节文件部分截断为 4,103 字节、截断为零、重新扩展为 8,193 字节全零数据，旧块 ID 不复用；先前的目录、TTL、控制者交接和 Worker 清理回归继续通过。

此前的完整 native-integration 路径验证为 30 项功能及全部文档测试通过；本次 Resize、删除发布优化及响应规范化的最终组合已由 `6a60ef60` 固定 Git 来源回归确认，结果见下节。本轮没有更新 PVE，没有执行当前版本百万验收或最终原版性能比较。


### 原始属性结果的副本一致性

复核发现紧凑属性命令在各副本重新编码 protobuf map，逻辑字段相同但响应字节可能因 HashMap 顺序不同。新增回归在修正前明确失败；SetAttr 响应现使用排序后的 xattr 编码，保留公开 wire tag/type 和已有缓存原字节。Master **260 通过、2 ignored**，两个优化集群用例再次通过。进程重启场景同时增加保留原始 Native 属性请求、删除后代改变目录状态、换主后逐字节核对原始响应；固定 Git 组合回归结果另记。

当前仍需处理批量写 API 和其他少数旧式 RPC、完整业务优先级/后台最低进度调度，以及网络分区/提交回执丢失等组合故障矩阵；没有将共享 ReadIndex FIFO 当作所有业务已经统一调度。


### 最终固定来源组合回归（代码 6a60ef60）

hsm-rs 的八个直接依赖和 lock 的 29 个 Curvine 包固定 `6a60ef60f6dc1b10f0d830a9f265e4a0d20a3b9a`，没有本地路径 patch，也没有改变其他依赖。`CARGO_TARGET_DIR=/work/cargo-target CXXFLAGS=-g0 cargo test --locked --workspace --features hsm-curvine/native-integration -j 6` 完成：**30 项功能测试通过、0 失败、2 ignored**，全部文档测试执行成功。五个 managed 场景和一个 native 搬运场景均通过。

增强的全进程重启用例保留磁盘目录并排除旧 Leader：首次 Exists 正确，部分截断后的保留文件 inode/属性/内容一致，递归删除不复活，Worker 清理得到确认；保存的原始 Native 属性请求在目录后代已经改变、换主和重放后，仍逐字节返回原始响应。Curvine 另有 **260 项 Master 测试、两个 TCP/Raft 集群用例**通过。原始日志为开发环境 `research/hsm-namespace-canonical-result-green.log` 与 `research/hsm-namespace-canonical-git-pinned.log`（相对于 `/work/jay/hsm`）。hsm-rs 的发布记录见 [验证文档](https://github.com/JayTsu-sh/hsm-rs/blob/main/docs/VALIDATION.md)。

此轮没有升级 PVE，也未执行当前代码的百万验收和最终原版性能比较。剩余是批量写 API、少数旧式 RPC 的迁移/保护边界、完整业务优先级和后台最低进度调度、组合故障矩阵；这些没有因资格证明合批而被标为完成。


### 追加里程碑：批量写、目录共享等待与加权业务调度

三个公开批量写 API 已使用单一 Native 身份、私有有界准备、条件原子发布和原始结果。创建/块分配保留遇错停止及成功前缀，关闭保留逐项结果；同一 Record 在一个公开批量请求中只推进一次 authority version。目录命令改为共享资格等待，apply 仍复核准备后出现的读者。实现细节、预算和限制见 [批量写入与前后台调度](native-batches-scheduling.md)。

AuthorityGate 按 Recall/前台/后台 4:8:1 轮次调度，分别限制接入名额，保留共享/独占隔离；Worker I/O 释放并重新排队验证。Native 自身最多 8 个前台轮次后执行一个已等待的 TTL 请求，避免独立队列内部遮蔽全局权重。

当前结构回归为 Master **268 项通过、2 ignored**，两个 TCP/Raft 集群用例通过；原始日志 `research/hsm-owned-batch-scheduling-receipt.log`。用例包含真实提交后指定会话回执丢失、改名后原身份重试、原始前缀错误及旧路径不复活。这个故障注入不等于网络分区。固定 Git 来源、进程重启/在线节点故障组合结果在后续确认后追加。

仍保留少数稀有/挂载 RPC 与普通 HSM 控制的独占边界；没有把加权接入称为所有业务效果都可并行发布。百万规模持续负载、组合 HA 故障矩阵及最终原版对比尚未执行；历史数字不代表本次结果。


### 在线换主发现的预检失败边界

独立进程测试暂停 Leader、由另外两个 Master 选主、恢复旧 Leader，再暂停替代 Leader 时，发现旧 Leader 因尚未提交的读批 ReadIndex 失败而主动退出，导致第二次丢失 quorum。原始失败日志 `research/hsm-live-ha-seal-fixed.log` 及其打印的 live-servers 日志保留；没有把这次运行记为通过。

ReadBatcher 的预检失败已向调用方返回错误，协调循环现正常结束该批，不再把它升级为“提交未知”并故障退出。控制操作、namespace 初始化和策略快照同样将无修改的资格预检放在提交未知边界之外；真正提交后无法确认前缀的故障退出仍保留。新增单元回归验证预检失败返回、RocksDB sequence 不变和全部接入名额释放。

修正后 Curvine **269 项 Master 测试通过、2 ignored**，两个 TCP/Raft 优化用例通过，日志 `research/hsm-preflight-no-abort-regression.log`。在线两次换主及固定 Git 组合结果尚需由 hsm-rs 运行确认；本轮百万和最终原版比较仍未执行。


### 固定 Git 的在线 Master 失联与恢复（fd2b109f）

修正预检边界后，固定 Git 来源的完整 hsm-rs workspace **31 项功能测试、3 ignored 及全部文档测试通过**，日志 `research/hsm-owned-batches-live-ha-fixed-git.log`。新增独立进程用例暂停当前 Leader、由两个存活节点选主、恢复旧 Leader，再暂停替代 Leader，要求恢复的旧节点参加下一次 quorum；完成 Archive/Release/Recall、读取并验证 2 MiB+29 字节及稳定 inode/data identity。批量成功前缀的原始错误在改名及两次选主后逐字节不变。原有的全进程重启用例另核对三个批量 API 的原始响应。

31 项的记录对应 fd2b109f 与执行器分流前的客户端代码；不能直接当作后续两通道 worker 的验收。

### 执行器分流与按类索引分页

增加 Source 的 work_class 分页，在既有 work:0:/work:1: 上 seek，不改变磁盘键和游标格式；跨类别游标拒绝。Master 回归增加 1,000 个 Recall 与两个后台任务混排的边界、分页和只读检查。hsm-rs 常驻 worker 分开运行 Recall 与 Archive/Release；原始 claim 与工作流结果继续由 Source 维护。详细行为见[批量写入与前后台调度](native-batches-scheduling.md)。执行器暂停后台、普通读自动 Recall 的组合回归另记。


### 执行通道组合验收（代码 222a3254）

Curvine **270 项 Master 测试通过、2 ignored**，两个 TCP/Raft 优化集群用例通过，日志 `research/hsm-work-class-pages-fixed.log`。hsm-rs 八个依赖及 29 个 Curvine Git source 固定 `222a32549739d91d9e685fad29647f9fe6532918`，没有本地路径 patch，其他 lock 项不变。完整 native-integration workspace **32 项功能测试通过、0 失败、3 ignored**，全部文档测试执行成功，日志 `research/hsm-worker-lanes-full-fixed-git.log`。客户端及测试提交：[d6e17ac](https://github.com/JayTsu-sh/hsm-rs/commit/d6e17ac9450d38001ecfdba38581736edc7829d2)。

七个 managed 场景通过，包含两次在线选主/旧 Leader 恢复，以及真实 CLI worker 的后台轮次暂停：普通读取独立自动 Recall、校验 1 MiB+37 字节，后台文件此时仍 Online；恢复后台后 Archive 完成。全进程重启回归核对三个批量 API 的原始结果与成功前缀，单独 native 搬运场景也通过。三个 ignored 为百万规模测试及两个父测试显式启动的子进程 fixture。

### 文件控制共享提交与 inode 统一预留

成功的文件级 HSM 控制、Seal 和只读控制已共享资格准备/等待 Raft。Native 与私有 Import 共用 inode 原子预留，apply 以原子 max 推进恢复 floor；失败准备可以留下 ID 空洞。新增 `hsm-conditional-v1` 封装：apply 前版本/路径/create 观察过期返回 Busy，不写入 Record、工作流绑定或永久 outcome；相同身份重新准备可成功。业务拒绝重新取得独占资格并从头检查后才持久保存。旧日志仍重放旧语义，新增空 CF `hsm_native_v7` 阻止旧二进制误开 checkpoint。详情见[文件级条件提交](conditional-controls.md)。

真实共享进展回归同时发起 16 个 HSM Import 和 16 个 Native 创建，并保持一个共享资格不退出，检查 32 个 inode 均不同。它首先暴露 SDK 绑定已有 namespace 仍申请独占资格：`research/hsm-conditional-control-fixed.log` 的集群测试超时。修复为“仅创建/升级独占、绑定已有 namespace 共享读取”后通过，未放宽测试边界。最终完整 Root 回归日志 `research/hsm-conditional-final-root.log`：**276 项 Master 通过、0 失败、2 ignored，两个 TCP/Raft 用例通过（38.85 秒）**。还覆盖新读者使已准备 Release 失效且租约不丢失、暂态拒绝不写 RocksDB/绑定、原身份重试、历史持久冲突兼容、路径观察失效、格式栅栏与并发 ID 预留。

以上日志相对 `/work/jay/hsm`，没有运行百万规模和最终性能比较。固定 Git 的 hsm-rs 组合验证另记。

### 条件提交的固定 Git 组合验证（代码 ad3391bd）

hsm-rs 的八个直接依赖及 lock 的 29 个 Curvine 包固定 `ad3391bd3cc21cf6ab6f721026c3cc069bbfa3f1`，其他依赖不变，无本地路径覆盖。完整 `--locked --workspace --features hsm-curvine/native-integration -j 6` 回归 **32 项功能通过、0 失败、3 ignored，四个 crate 的全部文档测试执行成功**；日志 `research/hsm-conditional-fixed-git.log`。七个 managed 场景通过（86.71 秒），包含全进程冷恢复、两次独立 Master 在线选主/恢复、后台暂停期间自动 Recall；单独 native 搬运用例通过（17.81 秒）。配套 hsm-rs 提交为 [70853a1](https://github.com/JayTsu-sh/hsm-rs/commit/70853a11edb10a009a8995def243916fbc8f8572)。

Root 的 276 项 Master 与两个 TCP/Raft 用例为同一生产代码的验证；最后仅补充文档。两个仓库格式/差异检查通过，已推送 Jay fork/main 及 hsm-rs/main。未执行新版本百万负载、最终性能对比或 PVE 升级，未将物理 HA 能力计为已完成。

### 生命周期共享提交、跨记录检查与 Recall 认领优先级

策略更新、扫描入队、任务 claim/finish 和显式 GC 入口已共享资格等待；namespace 初始化/升级、controller epoch 更新及业务拒绝重新确认仍按原规则独占。新增独立 lifecycle 条件封装与 `hsm_native_v8` checkpoint 栅栏，记录最多 16 个文件版本/当前路径/副本数、3 个公共键观察，并复核最多 34 个控制键，总编码上限 1 MiB。历史日志与 v7 文件封装语义不变。详情见[生命周期共享条件提交](lifecycle-shared-commit.md)。

容量策略检查已复制的待释放预算旧值；水位样本随命令保留，不能当作物理空间强预留。claim/finish 还检查动作结果的存在性，防止准备失败报告之后成功动作已经提交，却仍把任务标为失败。Source 用当前 namespace 路径筛选 claim。Recall 的 claim/finish/get 按任务实际 action 选择独立接入名额和相同的 gate 类别，后台名额满时不会退回后台队列。

初始 Root 回归 `research/hsm-lifecycle-readset-regression.log` 为 282 项 Master 和两个 TCP/Raft 用例通过；追加实际 Source claim 暂停/读取插入的回归 `research/hsm-lifecycle-claim-race.log` 也通过。最终增加按任务 action 的 Recall 接入测试，日志 `research/hsm-lifecycle-priority-final.log`：**283 项 Master 通过、0 失败、2 ignored，两个 TCP/Raft 用例通过（36.80 秒）**。真实集群在持有另一共享资格时执行 Seal、policy_put、policy_scan、claim、finish、GC；暂停实际 claim 的准备后让普通读取提交，旧请求返回 Busy，原身份重试得到 Superseded。暂停 hook 只在 hsm-scale-test 编译。固定 Git 的完整 hsm-rs 组合验证另记。

### 生命周期的固定 Git 与在线任务恢复验收（代码 d5e88f1d）

hsm-rs 八个直接依赖及 lock 的 29 个 Curvine 包固定 `d5e88f1d03f399ffd31099b8a4887ecfea2fbc7e`，其余锁文件内容不变，没有路径覆盖。最终完整命令 `CARGO_TARGET_DIR=/work/cargo-target CXXFLAGS=-g0 cargo test --locked --workspace --features hsm-curvine/native-integration -j 6`：**32 项功能通过、0 失败、3 ignored，四个 crate 的全部文档测试执行成功**；日志 `research/hsm-lifecycle-fixed-git-final.log`。七个 managed 用例 88.49 秒，单独 native 用例 23.80 秒。配套提交 [0c94b86](https://github.com/JayTsu-sh/hsm-rs/commit/0c94b869a9026efcfec0a0b17d7846b6a8dfbea2)。Root 283 项 Master 和两个 TCP/Raft 用例是相同生产代码的验收。

增强的在线 HA 先写入约 2 MiB 并实际归档，在 Source 保存策略、入队和 Running claim，再暂停 Leader。两个存活节点选主后，原 claim 请求返回原 token，原 policy_put 返回原成功结果；普通读取废弃旧 Release 观察，执行器报告 Superseded 后，由策略重选当前观察，真实 Release 并以 Source 成功结果 finish 为 Done/Offline。恢复旧 Master、暂停替代 Leader并再次选主后，任务 Done 与原 finish 保留；真实 Recall 和完整内容/inode 校验后，重试旧 finish 仍返回原 Offline 结果，不能重新生成当前 Dual 状态。

该用例不在外部重建任务镜像，也不把 Master term 当作 controller epoch。已有冷恢复、策略 CLI、后台暂停期间自动 Recall 等用例继续通过。两个仓库格式/差异检查通过，代码已推送 Jay fork/main 与 hsm-rs/main。未升级 PVE，未执行新版本百万负载和最终原版比较，未将 Worker/磁盘丢失或归档冗余算作通过。

### 当前剩余边界

已完成公开批量写、目录共享等待、加权资格、外部执行通道、文件级控制共享提交与两次在线选主验证；**没有将全部性能优化标为完成**。

1. 文件级 HSM、Seal、策略/任务和 GC 的共享提交已完成；namespace 创建/升级、controller epoch 变更及业务拒绝重新确认的独占是保留的全局协调边界。兼容原始 RPC 仍通过旧投影隔离，不能将它们描述为 owned 命令。
2. 旧后台收敛仍需处理：失联 Worker 已改为最多 1024 项一页、页间重新排队，并按本次失联 token 和原始 Leader 任期取消旧任务；UFS 缓存失效仍在该页独占资格下确认，部分 TTL/UFS 动作也保留旧路径。普通无 UFS、已纳管文件的 TTL 旁路与原生目录 TTL 删除已在 owned 路径；仍不能把旧式 Worker 副作用全部描述为持久 outbox。
3. 网络选择性分区、Worker/磁盘丢失、归档冗余和控制者自动接管不因 Master 暂停/恢复通过而完成。尤其单副本私有 Recall 的物理 HA 保持原限制。
4. 本轮没有升级 PVE，没有执行新版本百万规模持续负载验收或最终原版性能比较。完成剩余优化后再进行统一对照，不复用历史性能数字。


## 失联 Worker 的分页清理

生产 heartbeat 清理已接入按字节游标读取/删除双向位置索引，每页最多 1024 项且不填充 block cache。副本修复逐页交接，不再预先聚合整个 Worker 的块清单；页间释放 authority gate，下一页按 Background 类重新排队。普通 Curvine 文件不会触发空缓存失效 WriteBatch，也不等待没有日志的 journal 回执；UFS 修改与可能已有部分效果的错误仍保留确认。详细顺序、重连取消及单个大 UFS 文件仍有成本的边界见[分页清理设计](lost-worker-cleanup.md)。

每次失联使用新 token，Start/Running/End 或新位置报告使旧任务失效；检查与删除之间由 WorkerManager 读锁保护。所有页绑定最初 Leader 任期，不能在同进程再次当选后继续旧任务。token 是本地取消身份，不新增外部镜像，也不是持久 HSM 状态。UFS 缓存判断排除所有该 Worker 的位置，包括后续页，以免边界处误判缓存可读。

这次没有把 legacy UFS namespace 失效改为 owned 条件命令，Worker 删除也尚未全部具备持久 outbox；1024 项不是单个大文件解码或 UFS 失效的时间硬上限。全部优化完成前，继续暂缓新版百万验收和最终原版性能对照。

最终 Source 回归通过：**290 项 Master、2 项真实 TCP/Raft、64 项文件系统与 6 项 inode 测试，0 失败**。纯位置清理无需 journal 回执的专项测试、清理暂停时真实 SDK 写入/读回、原有 UFS 缓存失效与 Follower journal 重放均通过。原始日志为开发环境 `research/hsm-worker-cleanup-root-final.log`（相对 `/work/jay/hsm`）；匹配外部组件的固定 Git 回归另行记录。
