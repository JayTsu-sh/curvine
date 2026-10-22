# HSM 权威记录与候选索引：RocksDB 实现设计

2026-10-20 编写，2026-10-22 随提交与写入链路优化更新，描述当前实际实现。源码入口为 `RocksInodeStore`、`hsm/ordinary.rs`、`hsm/lifecycle.rs` 与 `apply_hsm`。本文区分已经实现的行为和尚未优化的成本；不是另一套计划中的数据库方案。

## 1. 数据库、CF 与权威边界

每个 Master 使用自己的元数据 RocksDB。现有 CF 为 `inodes`、`edges`、`block`、`location`、`common`。HSM 业务数据复用 **common CF**；另有 `hsm_native_v2/v3/v4/v5` 四个空 CF 作为旧二进制拒绝打开的格式栅栏。没有外部文件元数据数据库。

```text
Master 本地 RocksDB
├── inodes    原生 inode、文件长度与有序块信息
├── edges     原生父子目录关系
├── block     原生块与 Worker 位置映射
├── location  原生 Worker 与块的反向关系
└── common
    ├── 0x01 ...  原有挂载记录
    ├── 0x02 ...  原有锁记录
    └── 0x03 + "hsm:" ...  HSM 记录、派生索引与控制记录
```

下文 `P` 精确表示字节串 `b"\x03hsm:"`。`0x03` 是一个真实的前导字节，不是文本字符 `\\x03`，也不含空格。其后的键采用 UTF-8 文本；Record、访问 overlay 和原有 HSM 控制 value 使用 JSON；Native session、结果和块清理事实使用 bincode，GC 游标保存原始键字节。下文键表标明编码，原生其他 CF 不因此改为 JSON。

RocksDB 是已提交元数据的本地投影。三个 Master 的一致性来自原有 journal/Raft 提交与相同 apply 逻辑，不来自三个 RocksDB 自己互相同步。当前 RocksDB 配置关闭 WAL，不能把一次本地 WriteBatch 成功单独等同于跨节点持久成功。

## 2. 主记录与候选索引

| 项 | 精确键形式 | value | 角色 |
|---|---|---|---|
| HSM 主记录 | `P + record:<inode>` | JSON `Record` | 当前内容身份、驻留状态、归档依据及保护事实 |
| 候选索引 | `P + candidate:<s>:<atime20>:<inode>` | JSON 字符串形式的 inode，例如 `"1021"` | 派生排序入口；不保存完整 Record |

`<inode>` 是当前 Curvine 元数据实例内的十进制 inode ID；记录键不包含路径或 generation。同一文件的新内容版本更新同一主记录，DataKey 内的 generation 区分内容版本。storage ID、namespace incarnation 保存在记录身份中，不重复放进每个索引键。

候选索引覆盖已纳管记录，包含 Online、Dual、Offline；有索引项不等于已经符合某条策略。writing、保护、大小等条件在读取主记录后判断。

### 2.1 索引键的逐段编码

```text
P + "candidate:" + s + ":" + format(atime_ms.max(0), "020d") + ":" + inode

s = o → Online
s = d → Dual
s = f → Offline

示例：inode=1021，atime_ms=1792485229722
key   = b"\x03hsm:candidate:o:00000001792485229722:1021"
value = b'"1021"'
```

时间戳以毫秒为单位，用 **20 位十进制左补零**。同一状态前缀内，字节序与非负访问时间顺序一致，因此先读到较老访问时间。inode 尾段未补零，同一时间内按文本字节序排列，不保证 inode 数值序。

Release 分别扫描 `candidate:d:`，再扫描 `candidate:o:`；这个优先级由策略代码选择前缀实现，不依赖对全部状态键做一次全局字典序扫描。Archive 只扫描 `o`，管理用 Recall 策略扫描 `f`；普通读自动 Recall 不需要该策略扫描。

当前索引没有包含 policy ID、路径、owner、大小或任意热度分数，也没有为每条规则复制一套候选表。正的访问年龄权重等价于该状态范围内的冷度排序，不提供多维评分表达式。

稳定 `record:<inode>` 与高频 `access:<inode>` 分开保存。后者包含 base_version、version、公开 revision、atime、readers 与读时钟；查询必须经 `hsm_record` 合成。稳定字段变化时同批物化访问状态、删 overlay 并维护精确候选键；不能单独读取 base JSON 推断当前读取保护。

## 3. Record 的 value 结构

以下为**结构样例**，并非数据库现场 dump。ID、修订号和时间仅用于说明字段；没有能力令牌示例值。

```json
{
  "ordinary": true,
  "native_blocks": true,
  "sealed": false,
  "seal_token": null,
  "atime_ms": 1792485229722,
  "mtime_ms": 1792485230911,
  "owner": "root",
  "group": "root",
  "writing": false,
  "readers": {},
  "native_garbage": {},
  "state_since_ms": 1792485230911,
  "version": 2,
  "state": {
    "data": {
      "entity": {"storage": "<storage-id>", "namespace": "<incarnation>", "entity": "1021"},
      "generation": 1
    },
    "revision": 2,
    "residency": "Online",
    "length": 8388625,
    "digest": {"algorithm": "unsealed", "value": []},
    "anchors": []
  },
  "path": "/data/a",
  "published": null,
  "pending": null,
  "views": {},
  "garbage": []
}
```

| 字段组 | 用途与更新约束 |
|---|---|
| `state.data` | storage + namespace + inode + generation；绑定准确内容版本，路径不作为归档身份 |
| `state.residency/length/digest/anchors` | 驻留事实、逻辑长度、摘要与已提交归档回执；anchors 不存搬运进度 |
| `version` | 内部 Record CAS 观察；保护字段变化也可推进 |
| `state.revision` | 公开 EntityState 的修订，供条件效果检查；读取也更新观察 |
| `state_since_ms` | 当前驻留区间的时间依据，供最短驻留条件使用 |
| `ordinary/native_blocks/writing` | 是否普通文件、是否原生块布局、是否仍在写入；writing 文件不归档 |
| `sealed/seal_token` | 普通内容摘要是否已确认，以及校验过程的持久保护；不是逐次扫描全文件哈希 |
| `path/atime_ms/mtime_ms/owner/group` | 同一权威域内供策略使用的属性投影；扫描还按 inode 获取原生当前路径 |
| `views/readers` | 稳定归档视图和读取租约；有有效保护时不得 Release |
| `pending/published` | 私有恢复中的目标和已发布 HSM 布局；普通原生块以 native_blocks 表示，不要求 published 非空 |
| `garbage/native_garbage` | 已撤销但仍需物理删除的私有布局或普通块副本，不能当作在线引用 |

Record 当前没有独立的 schema_version 字段。新增字段的读取兼容依赖 serde 默认值，普通钩子的行为激活依赖 namespace.ordinary_format=2；namespace:v1 的键版本、卷格式标记、内容 generation 和 Record 修订号分别表示不同维度，不能互相替代。独立的逐记录 schema 迁移框架尚未实现。

对外 `status` 返回 `record.state`，不返回完整内部 Record。普通 CompleteFileResponse 只有 result 和可选 file_blocks；FUSE close 不携带 Online 或上述内部结构。

## 4. 原子维护：主记录与索引同一个 WriteBatch

`ordinary::write_record(batch, old, new)` 的实际顺序：

```text
Put(common, record_key(new.inode), JSON(new))
若 old 不存在或旧、新候选键不同：
    若 old 存在：Delete(common, candidate_key(old))
    Put(common, candidate_key(new), JSON(new.inode_as_string))

seal 的旧、新派生值 = (seal_token 非空且 sealed=false) 时的 DataKey，否则为空
仅在派生值变化时 Put 或 Delete seal:<inode>

gc 的旧、新派生值 = (garbage 或 native_garbage 非空) 时的 EntityKey，否则为空
仅在派生值变化时 Put 或 Delete gc:<inode>

由调用方 Commit 一次 WriteBatch
```

所以同一提交批次不会只留下“新 Record + 旧候选键”。状态和 atime 不变时，保留原候选项，不再删除或重写。Record、候选项及 seal/GC 派生键在稳定投影中对应；派生键可以由 Record 重建，但 Record 中的 pin、回执和 pending 等事实必须来自提交历史，不能丢弃后靠扫描业务目录推测。

当前 SDK 的创建/覆盖、块分配、写打开和 flush/close 已进入 Native committed 链路。准备只生成触及 inode/edge 的 owned 变更，Raft 提交后 Leader/Follower 同样 apply；flush/close 与覆盖涉及的原生 inode、块位置、HSM Record/候选项、GC 事实和原始结果在同一个 WriteBatch 更新。块分配不逐块重写稳定 Record。每个命令检查 inode 摘要、HSM version 及所观察控制字节；批内前序效果对后序准备可见。

rename/delete/部分其他原生及后台修改仍在过渡路径，不能把上述同批保证推广到所有 RPC。过渡路径共享相同协调 gate，等待自身 journal 前缀确认；完整去掉该 gate 与混合提交仍未完成。

当前不是 RocksDB TransactionDB 的外部乐观事务。HSM 条件提交由 apply 中的 expected epoch、Record.version、record_checks、control 原字节比较实现，顺序 apply 后决定是否更新。

## 5. 不同操作如何迁移索引

| 触发 | 主记录变化 | 候选索引变化 |
|---|---|---|
| 新普通文件完成写入 | Online、writing=false、unsealed、anchors=[] | 建立 `candidate:o:<atime20>:<inode>` |
| 普通读打开、有效续租/关闭 | 更新 atime、公开 revision、读取租约及内部 version | 删除旧 atime 键，再写新 atime 键；旧 Release 观察失效 |
| Archive commit | 登记当前 DataKey 的回执，Online → Dual | `o:<t>:<id>` → `d:<t>:<id>` |
| Release commit | 撤销在线块列表，Dual → Offline，保存 native_garbage | `d:<t>:<id>` → `f:<t>:<id>`，同时建立 GC outbox |
| Recall commit | Seal 后原 inode 发布，Offline → Dual | `f:<t>:<id>` → `d:<t>:<id>`；随后的读更新 atime |
| 原生布局覆盖/截断 | generation 推进，撤销旧 anchors/digest，重置摘要确认 | 更新对应 Online 候选；writing=true 时仍须后过滤 |
| 原生布局 rename/属性更新 | 更新 path/策略属性与版本 | 调用通用记录维护；排序字段未变则保留索引项 |
| 允许的文件删除 | 删除对应 HSM Record | 同批删除对应候选键；有未清理垃圾时先拒绝删除 |

索引不保存文件数据块，也不直接删除 Worker 内容。Release 的原生块撤销、Offline 和持久 GC 事实先提交；物理空间随后由 Worker 删除任务回收。

## 6. 策略如何使用这个索引

1. 从 `policy:<id>` 和 `scan:<id>:<revision>` 读取规则与进度，先判断周期、水位及在途预算。
2. 根据动作选择状态前缀，Release 先 d 再 o。
3. `hsm_scan(prefix, after, limit+1)` 对 common CF 做有界 range_scan。每页**处理最多 256 项**，额外预读 1 项判断是否还有下一页；不是最多发生 256 次物理读取的保证。
4. 从索引 value 解析 inode，再 Get `record:<inode>`。先判断访问时间范围与 writing、大小、owner/group、修改/驻留年龄、保护和动作状态；满足这些条件后才按 inode 解析当前原生路径，检查路径前缀与排除条件。满足访问时间提前结束条件时，转到下一状态范围。
5. 未 sealed 的普通候选返回 to_seal；外部执行器完成受保护摘要后再选择。索引项不是直接发送给 data-mover 的数据计划。
6. 入队 transaction 同时核对 policy 原值、record version、intent 键和实体 active 键；成立才写 Ready、active、工作索引、回收预算与扫描进度。

扫描游标是上次索引 key 的文本形式，JSON 中前导控制字节转义为 `\u0003`。`hsm_scan` 用 after 后补一个 0 字节作为下一页起点，prefix 最后一个字节加一作为排他上界。候选键无内嵌 NUL，按完整候选键继续可排除上一项。

分页不是全 namespace 一致性快照。atime 或状态变化会把键移到游标前后，可能到下一轮才看到，去重与 source CAS 防止旧观察产生错误效果。scan_budget 约束每轮计入 examined 的候选数，额外 lookahead、Record point-get、路径解析和控制记录查询仍有成本。无需 readdir，但不等于没有元数据 I/O。

## 7. 相关控制键空间

以下均位于同一 common CF，value 为 JSON。它们不是候选表，也不是每次普通写入都会创建的空记录。

| `P` 后缀 | value / 职责 |
|---|---|
| `namespace:v1` | namespace 身份、controller epoch、ordinary_format；键的 v1 不等于 ordinary_format=1 |
| `enroll:<inode>` | JSON inode 数值；namespace 尚未初始化时的待纳管记录 |
| `seal:<inode>` | DataKey；中断摘要校验的恢复入口 |
| `gc:<inode>` | EntityKey；待清理布局/块的恢复入口，具体垃圾在 Record 内 |
| `policy:<id>` | LifecyclePolicy，内含 revision 与 selection |
| `scan:<id>:<revision>` | PolicyRun：cursor、next_due_ms、admitted、pressure |
| `intent:<64位id>` | LifecycleIntent：expected、动作、状态、claim、result 等 |
| `active:<inode>` | JSON intent ID；实体当前未完成动作的去重入口 |
| `work:<priority>:<intent-id>` | JSON intent ID；Ready/Running 工作入口，Recall=0，其他=1 |
| `reserved:online:<intent-id>` | JSON 预计回收字节数；跨规则共享预算，不是硬空间预留 |
| `binding:<hash>` | 全局操作 ID 与动作/DataKey 的绑定 |
| `outcome:<hash>` | 原始指纹、成功或确定拒绝响应、内容身份与动作，用于幂等恢复 |

completed intent 不参与每轮 Ready/Running 工作索引扫描。共享预计回收预算由 `budget:online:v1` 汇总项与 reservation 同批增减，正常查询 O(1)；旧 checkpoint 缺项时分页重建。它不代表 Recall 的硬容量预留。

### 7.1 Native session 与物理块清理（common CF）

以下键同样带 `P` 前缀，value 不使用 JSON。

| 键尾 | 编码与内容 | 更新方式 |
|---|---|---|
| `native-session:<session>` | bincode：acknowledged、highest、retained_bytes | 原始业务结果同批维护；1024 序号、32 MiB 窗口 |
| `native-result:<session>:<seq20>` | bincode：指纹、原始编码回复与结果类型 | 与业务效果同批；每次最多清理 8 条已 ack 结果 |
| `native-session-time:<issued20>:<session>` | 会话时间索引 | 每命令最多清理 4 个过期会话范围 |
| `native-session-floor:v1` | bincode 毫秒下界 | 同批推进；时钟回退也不复活过期身份 |
| `native-inode-floor:v1` | bincode i64 分配上限 | 新 inode/旧日志分配同批推进，删除不降低 |
| `native-gc:v1:<block_hex16>` | bincode `Garbage { incarnation, block, locations }` | 与撤销旧布局同批写入；独立于 inode/Record 生命周期 |
| `native-gc-cursor:v1` | 原始 GC 键字节 | 每页最多 16 个事实，维护提交推进，末页回绕 |

块 ID 以无符号 64 位的 16 位十六进制排序编码，覆盖有符号 i64 的完整位模式。覆盖写保留 inode 的 next_seq，旧 AddBlock 回放也从布局恢复序号下界。新内容不能复用 GC 中的旧块 ID。

退休时将预分配与已上报位置合并，在 `block/location` CF 写入哨兵，避免把尚无上报的副本当作已删除。外部 `gc_batch` 根据已提交事实给 Worker 下发幂等删除；观察删除确认后，把原事实字节、namespace incarnation/epoch 及游标送入 Native journal 的维护分支。apply 仅删除仍匹配的事实，并清除本地旧位置；不依赖原 inode 仍存在，也不为每块保留永久请求结果。批次未知仍需建立提交前缀隔离，否则停止服务。

Release 的 `Record.native_garbage` 也在同批建立每块索引与副本哨兵；旧 checkpoint 缺该索引时仍能从 Record 判断清理义务。此索引不复制文件属性、路径或访问时间，属于相同存储权威域中的清理恢复入口。清理集合不变时不重写事实，也不会把已确认的副本位置重新加入。

HSM 卷处理块报告时先判断块是否仍在当前 inode 的布局中。退休中的旧块会收到删除命令，位置仍保留到 Deleted/完整报告对账；inode 已删除但仍有退休事实时同样按 Retired 处理；退休已结束的过时上报会被拒绝登记。GC 给原清单与当前报告位置的并集下发删除，覆盖准备后新出现的副本。

Native journal v1/v2 使用冻结 DTO 回放；v3 增加清理事实控制及维护确认。v4 保持 v3 字段编码并激活单 inode 删除效果，v1/v2/v3 历史继续可读；公共 NativeMutation RPC envelope 仍为 v1。格式 CF 是二进制降级栅栏，不保存另一份元数据镜像。

非递归 Delete 的原子 batch 同时删除目录项/最终 inode、HSM Record、访问 overlay、候选及旧 seal/GC 辅助索引，保存每块独立退休事实、位置哨兵和原始删除结果。多链接 inode 仅移除本次目录项并更新链接数，最后一条链接才删除 inode。空目录在 apply 重新检查 children，不能仅凭不含子项的 metadata hash 推断为空。物理删除由 `gc_batch` 异步推进，不要求调用者持有已删除 inode。递归删除尚未迁移。

## 8. 一致性、提交与重放

正常 HSM 事务通过 `JournalEntry::Hsm` 提交。apply 先查原 outcome，避免重复效果，再验证 controller epoch、expected_version、候选 record_checks 及 control 键旧值。成立时在一个 WriteBatch 更新记录、派生索引、必要原生 inode、控制键与 outcome；条件失败保留原始拒绝结果而不修改目标状态。临时 Busy 的重试语义另由动作入口处理。

当前 FUSE/SDK 完成路径以 Native owned 命令原子纳管；旧 CompleteFile 日志在重放时仍执行原纳管钩子。ordinary_format=2 用于新增普通钩子的版本激活，旧日志按旧格式语义解释，不能因为新增回调改变历史 Record.version。

HSM 卷冷启动用一个 WriteBatch 的 7 个范围墓碑清除原生 inode/edge/block/location、HSM 前缀及 mount/lock 投影，然后按 checkpoint 与尾部日志重建，保留其他 common 事实。restore 等待短快照读租约排空。清理不先收集全部键，不代表重放或内存 namespace 构建不再随实际恢复内容增长。旧 outcome 不会跨恢复点残留并错误跳过原生恢复；支持的 checkpoint 必须与相同 applied index 对齐。

## 9. 当前成本与实施边界

- 每个 Record 对应一个当前状态候选项；真实 atime 变化更新小型 access JSON 和原生 atime，并迁移候选键。排序键不变时只写 Record；seal/GC 派生值不变时也不写索引。公开访问修订与 Release 的版本失效语义保持。
- 时间索引可减少热记录扫描；大小、owner 等仍需 Record 回查，但会在解析路径前拒绝；路径条件仍后过滤，密集不匹配前缀可能跨多轮消耗预算。没有 path×atime 复合索引或按规则物化候选表。
- 稳定视图、锚点和垃圾仍保留在稳定 Record；高频读取租约与访问观察已拆入 access overlay，大集合仍需规模压测。
- 启动清理改用范围墓碑，不逐键构建应用侧删除批次。RocksDB 后台 compaction、cache/memtable、namespace 构建及日志重放仍有实际规模成本；共享预算正常读取为 O(1)。实际规模验证范围见 [规模测试记录](scale-validation.md)，不据此承诺生产 HA。
- 同批更新与日志恢复已经实现；自动对账/修复损坏候选索引的在线运维工具、独立 schema 迁移框架、终态历史自动保留清理并未在此声明完成。

### 9.1 三项成本分别影响什么

下表保留固定源码 `039631f4` 的优化前成本分析；当前代码已实现下节第一步及元数据条件前置。量化结果见 [规模测试记录](scale-validation.md)。需要区分三个数量：`N` 是受管文件数，`K` 是全部 HSM KV 数，`E` 是恢复点之后需要重放的日志数。任务、outcome 和保护事实也占键，因此 `K` 不等于 `N`；高频访问和历史动作会增加 `E`。

| 成本 | 当前实际工作 | 主要影响 |
|---|---|---|
| 访问更新 | 受保护读取的打开、续租及有效关闭会更新 Record 和访问观察；atime 改变时删除旧候选键、插入新候选键 | Raft/journal、JSON 编码、RocksDB 写入与 compaction 放大；实际访问更新率比单独的文件数更关键 |
| 条件后过滤 | 每页先读取最多 256 个索引项及一个 lookahead，再回查 Record 和原生路径，然后检查条件 | 入选比例低时，多轮扫描消耗预算并延迟策略发现；分页限制单次工作量，不保证发现一个合格文件的总成本 |
| 启动清理与恢复 | 清理先把全部 HSM 键收集到 Vec，再形成一次删除批次；之后从支持的提交历史恢复 | 清理时间、应用侧键集合及批次内存随 K 增长；恢复还受 checkpoint 大小与后续 E 影响，不能只用 N 预测 |

此外，容量策略的 `outstanding_bytes()` 会分页累加所有 `reserved:online:` 项。该扫描的内存有界，但每次计算的总工作量随在途任务数增长；规模测试也必须覆盖，不能只测候选索引。

### 9.2 优化顺序与安全约束

**第一步（已实现）：消除无界清理和不必要的索引写入。**

- 将启动清理改为按键数与累计字节数限制的批次；每批读取后释放 iterator，再提交删除。清理结束前保持恢复阶段，不提供 HSM 服务。中途退出后，下次启动重复清理仍应安全。原生 inode、挂载和锁记录必须保留。
- 另一种可评估方案是在同一写选项下对 HSM 精确前缀执行范围删除。范围 tombstone 仅使逻辑删除的提交很小，物理清理仍由 compaction 承担；必须测量随后的读取、重放和磁盘回收成本后再选用。
- `write_record` 比较旧、新候选键：相同就保留索引项；发生状态或 atime 变化才迁移。seal/GC 派生项也按内容变化更新，避免每次修改都删除不存在的 outbox。Record 的权威修订和 CAS 语义保持。
- 这一步减少冗余写，**不会消除真实 atime 变化所需的索引迁移**。不能仅为了少写索引就跳过访问观察更新，否则旧 Release 可能继续执行。

**第二步（条件前置已实现，新增索引待评估）：降低后过滤的无效工作。**

先把与路径无关的条件放在原生路径解析之前，包括写入状态、大小、owner/group、时间和保护事实；符合这些条件后再解析当前路径。路径必须来自当前 namespace，不能因缓存路径减少查询而漏掉祖先目录重命名。

增加每次扫描的计数与耗时：索引项、Record 回查、路径解析、各类拒绝原因、待 seal 数、实际入队数及提交等待。计数用于识别瓶颈，不直接等同于底层物理 I/O 次数。

只有当低选择率确实成为主要成本，才引入按目录范围或 owner 等维度组织的第二访问路径。新增索引需要一起解决普通写入、属性修改、文件及祖先目录 rename、删除、日志重放和在线重建。索引未就绪时应继续走现有有界扫描，候选进入任务仍由权威 Record/CAS 检查。不能为每个条件组合都建立索引，避免把扫描成本全部转成写入成本。

按分钟合并访问时间、按时间桶排序等属于后续设计选项。必须明确允许的时间精度、桶内排序、跨页扫描及提前结束规则，同时保留每次访问使旧 Release 失效的权威观察。当前精确 atime 语义下，不把时间桶当成已经可直接替换的优化。

**第三步：用完整 checkpoint 与有限日志尾部控制恢复时间。**

有界清理解决应用侧无界内存，不保证总恢复耗时不再随数据量增长。后续需核对 checkpoint 是否同时覆盖原生 namespace、HSM Record、索引、任务/outcome 及其对应的 applied index，再重放该点之后的日志。本轮已验证批量预置百万 namespace 后从 follower checkpoint 恢复、排除原 leader 并核对完整内容，见规模报告；完整故障矩阵和恢复 SLO 仍需按部署环境验收。

当前不能直接保留未校验的旧 HSM 投影：旧 outcome 可能让操作跳过与 namespace 对应的重放。对投影保留或增量恢复的改造，需要证明它与同一个提交恢复点一致，并覆盖 checkpoint 传输、崩溃中断、旧控制者及重试窗口。

终态 intent、outcome 和 binding 的历史保留也要单独设计。删除幂等结果可能让旧操作重新执行，需要重试保留窗口或持久拒绝旧操作的机制；不能为缩小数据库而直接定期删除。

### 9.3 百万规模测试应如何进行

分两层执行。**真实 RocksDB 的百万条合成 Record/索引基准**用于定位元数据算法成本；**真实 Curvine 集群的百万文件测试**用于验收完整链路。前者通过不能写成后者通过。

| 测试维度 | 数据与操作 | 必须记录的结果 |
|---|---|---|
| 数据规模 | 1 万、10 万、100 万受管记录；分别记录文件数、总 KV 数和数据库大小 | 主机 CPU/内存、存储类型、进程数、版本、配置、初始 compaction 状态 |
| 访问更新 | 单热文件重复读、固定热点集合、均匀访问；分别测打开、续租、关闭及未改变排序键的更新 | RPC P50/P95/P99、吞吐、Raft 提交/等待、JSON 字节数、实际写入与 compaction 字节、峰值 RSS |
| 策略扫描 | 高选择率、1%/0.1% 选择率及零匹配；路径、owner、大小分别拒绝；加入新变冷文件 | 每页延迟、完整一轮耗时、首个及全部合格项的发现延迟、检查/入选比例、Record/路径查询数 |
| 持续并发 | 普通写入和读访问持续进行，同时运行 Archive/Release 与自动 Recall | 前台 P99、扫描对前台的影响、重复任务、旧 Release 被拒绝、自动 Recall 合并情况 |
| 历史规模 | 文件数相同，在途任务数、终态历史数和 checkpoint 后日志量不同 | 容量预算计算耗时、恢复日志数与耗时；避免把历史规模误算成文件规模 |
| 启动与恢复 | 正常停机、强制退出、清理中断、恢复中断；有/无可用 checkpoint；不同日志尾部 | 清理/恢复分段耗时、总可服务时间、峰值 RSS、最大批次键数/字节数、恢复状态与任务结果一致性 |

规模基准使用独立测试目录或测试集群。分开报告新进程、预热后的运行，以及明确控制的冷缓存条件；只重启进程不能声称操作系统缓存也已清空。每组重复测量并保存原始数据，比较同一环境下的优化前后结果。

在真实集群层，用普通 FUSE/SDK 创建和访问文件，让数据经过真实写入纳管、策略筛选、归档、Release、GC、访问自动 Recall。若为控制运行时间只选择其中一部分文件搬运，应明确区分“百万文件 namespace”和“实际完成完整 HSM 流程的文件数”，不能合并为一个验收数字。

### 9.4 何时可以改写当前限制

验收至少满足：清理应用侧峰值内存由固定批次限制；零匹配策略能够持续分页并最终完成一轮；测试中的合格项没有永久漏选；访问与 Release 竞争仍安全；重启前后文件身份、驻留、归档锚点、保护和任务结果一致；同时给出吞吐、前台延迟和恢复耗时。

绝对吞吐、P99 与恢复时间需要按目标硬件及业务 SLO 确定。只有元数据基准或少量闭环通过时，应保留“尚未做百万文件完整集群性能验收”的限定。本节区分已实现的差量索引、有界清理和条件前置，与后续复合索引、历史保留等工作。**百万记录基准、批量预置的百万 namespace、百万普通客户端写入是不同验收范围**，结果按 [规模测试记录](scale-validation.md) 分别报告。

## 10. 源码落点

| 位置 | 实现 |
|---|---|
| `master/meta/store/rocks_inode_store.rs` | CF、关闭 WAL、HSM get/range_scan/batch、投影清理 |
| `master/hsm/ordinary.rs` | 候选键、Record/索引/outbox 同批维护、纳管、读取和删除钩子 |
| `master/hsm/lifecycle.rs` | 状态范围、分页回查、筛选、CAS 入队、PolicyRun 和工作索引 |
| `master/hsm.rs` | namespace/record/outcome 键、条件 apply、inode/控制记录同批提交、屏障 |
| `master/journal/journal_loader.rs` | 普通与 HSM 提交重放 |
| `curvine-model/src/hsm/native.rs` | Record、Namespace、Transaction、ControlChange |
| `hsm-core/src/managed.rs` | DataKey、EntityState、驻留枚举与归档回执 |

源码路径前缀分别为 Curvine 的 `curvine-master/src/`、`crates/common/`，以及配套 hsm-rs 工作区。生命周期概览见 [lifecycle.md](lifecycle.md)。


### Native namespace v5 与格式栅栏 v6

`hsm_native_v6` 是新增的空兼容性 CF，业务数据仍在既有 inode/edge/block/location/common 中。它标识 compact namespace command：原始输入、稳定请求身份、捕获的操作/租约时间以及内部 TTL 条件进入 journal；不把全子树展开数据塞入日志。apply 生成的 inode/edge/HSM/access/candidate/每块退休事实与 session 原始结果同一 WriteBatch。目录移动后加载 FileEntry 对应的完整 inode 并更新 HSM 路径。

历史 Native v1–v4 的 DTO 不改变；v5 单独解码。CF v6 使旧进程不能误开新 checkpoint，仍要求全 Master 支持后启用，不提供混合版本滚动激活。失联 Worker 的派生位置正反索引以 1,024 块为一批同时移除；该位置清理不代表 Worker 磁盘删除确认。


### 文件控制条件封装与格式栅栏 v7

新增空 CF `hsm_native_v7`，没有新增元数据镜像或逐文件副本。新 `hsm-conditional-v1` 日志复用 Transaction 数据，专用于文件记录条件发布；版本、当前路径或 create 观察过期时，不修改投影、不创建 binding/outcome，返回 Busy。成功状态、Record/派生索引/退休事实与原始结果继续同一 WriteBatch。历史无此封装的 Transaction 仍按旧规则持久保存 CAS 冲突。

Native 创建和私有 Import 共用内存原子 ID 预留器；准备失败留下空洞。已有持久 inode floor 继续只由 committed apply 推进；并发预留超过重放 floor 时只保持较大值。已有 namespace 的 SDK 绑定是共享读取，创建/升级才独占。具体流程与回退边界见[文件级条件提交](conditional-controls.md)。


### 生命周期条件封装与格式栅栏 v8

新增空 CF `hsm_native_v8`，没有新增逐文件数据库。`hsm-lifecycle-conditional-v1` 在 journal 中携带有界检查集：namespace 与动作 outcome 的原始值、容量策略读到的 `budget:online:v1`、候选/claim 的 Record 合并版本、当前路径和副本数；Transaction 中的 policy/run/intent/active expected 值继续逐键核对。检查过期返回 Busy/Fenced，不写游标、任务、索引、预算、binding 或 outcome。成功仍沿用同一 WriteBatch。最多 16 个文件、3 个公共键、34 个控制键、1 MiB 编码。

水位仍基于 Worker 遥测样本，逻辑预算 CAS 防止并发发布重复消费旧观察，不代表物理容量强预留。历史 Transaction、read-batch、v7 文件封装的解释不变，全部 Master 必须匹配新代码。详细流程见[生命周期共享条件提交](lifecycle-shared-commit.md)。


## Worker 清理的持久工作记录

common CF 新增 `\x03hsm:worker-loss:v1:{worker:08x}`（JSON job/pending/cursor/term/rewind）与 `\x03hsm:worker-repair:v1:{block:016x}`（JSON block/observation）。它们是清理检查点和修复需求，不是 namespace 属性镜像。每页最多 1024 个块，pending 与需求同一个 CAS / WriteBatch 提交；新 observation 防止旧确认删除新失联需求。内部 envelope 为 hsm-worker-cleanup-v1，最大 1025 个写行、64 个 inode 检查和 2 MiB payload；空 CF hsm_native_v9 标记格式。所有 Master 必须匹配，旧 v8 opener 不能打开 v9 checkpoint。完整字段、接续顺序与健康确认边界见[详细设计](worker-cleanup-recovery.md)。
