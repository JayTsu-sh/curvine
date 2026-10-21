# HSM 性能优化方案

状态：继续实施，**尚未完成全部优化，也未开始本轮原版对比**。P1 普通日志自身回执、originating term 约束和单 RPC 普通 open 已实现；P2 仍是安全的普通请求过渡微批，统一 committed 状态机和普通请求持久去重尚未切换；读取资格已改为 Raft ReadIndex，等待本地 apply 而不追加空日志（38 项 Raft、15 项 HSM 单元回归及真实三 Master 读批处理测试通过）。P3 读取保护批处理、基准记录/高频访问状态拆分已经实现；P4 已实现后台快照筛查、版本/当前路径复核、小批入队及共享预算汇总；P5 已实现范围清理、读结果有界保留和二进制 Worker 通道/连接复用。其余边界及验收状态见下表。

## 当前实现与完成门槛（2026-10-21）

| 项目 | 本轮代码行为 | 仍需完成或验收 |
|---|---|---|
| 普通 open | `open_path` 在关闭 HSM 时直接返回普通块布局；不缓存“关闭”判断，避免换主后使用过期能力 | 全部优化结束后统一基准 |
| 读取保护 | 最多 32 个条件读命令合并一次日志；私有批内记录保证热 inode 的读者不会互相覆盖；提交后回执直接返回 apply 结果 | 混合原生/HSM 统一提交；真实故障注入 |
| 高频状态 | `record:{inode}` 保存稳定实体；`access:{inode}` 保存 base/version、revision、atime、readers；稳定变更原子合并并删除 overlay | 精确 atime 仍迁移候选索引、仍更新原生 inode，语义保持 |
| 租约 | 任期内单调时间；`read-clock:v1` 持久时间下界；新任期/恢复时对既有读者保守等待 120 秒；SDK 截止期从发请求时计算 | 节点暂停、墙钟跳变与换主的端到端组合故障测试；无日志 delegation 未启用 |
| 策略筛查 | 提交前缀后固定 RocksDB 快照，后台最多 4 个扫描；每页软预算 10 ms/2 MiB（允许首条进展），最多 256 行；不填充 RocksDB block cache | 前台持续 I/O 下的百万 namespace 验收；最终调度优先级 |
| 策略入队 | 每批最多 16 个候选；重新检查 policy/run、record version、当前路径、读保护和共享预算 | 已用无日志 ReadIndex 建立前后读取资格；P2 完成后缩短跨提交 gate |
| 空扫描页 | 手动纯查询不记工作流结果；周期页面只条件提交 cursor/pressure，通过 apply 回执返回，无永久 outcome/binding；真正入队继续持久保存结果 | 新旧日志/checkpoint 及取消/丢回执回归 |
| 容量预算 | `budget:online:v1` 与 Ready/Running/终态 reservation 同批增减；正常读取 O(1)；旧 checkpoint 缺项时有界分页重建 | 并发规则与换主压力测试；不是 Recall 硬容量预留 |
| 冷恢复 | 精确 `\x03hsm:` 前缀范围 tombstone，替代逐记录删除；快照读租约在 restore 前排空；随后按既有 checkpoint/尾日志恢复 | 完整百万恢复验收；namespace 构建和日志重放仍随实际恢复内容增长 |
| 读历史 | 时间身份 + 持久 floor 拒绝过期请求，每次最多回收 8 对普通读结果/索引；工作流原始成功结果不清理 | 旧 checkpoint 中无历史索引的 legacy outcome 不会被猜测删除 |
| 数据通道 | 1 MiB 原始二进制 body、有界 JSON 头；连接按 Worker/config 复用，失败/取消丢弃；私有布局、Seal、摘要校验仍保留 | 本轮完整回归后再测传输收益；单副本 Recall HA 不在此优化中假装解决 |

统一提交层采用两入口（submit/read）的内部模块和 SDK 私有适配；准备确定的 owned 变更集、稳定请求身份和原始回复，Leader/Follower 同样 apply。未选用“把全部旧 handler 放入双树影子环境”的方案：它还需隔离 TTL、统计、Worker 删除、mount 和后台写入，正确性改造面更大。不能仅删除当前 gate 或把非确定 handler 原样移到 follower apply。

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
