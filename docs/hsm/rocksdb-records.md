# HSM 权威记录与候选索引：RocksDB 实现设计

2026-10-20 编写，2026-10-21 随源码发布，描述当前实际实现。源码入口为 `RocksInodeStore`、`hsm/ordinary.rs`、`hsm/lifecycle.rs` 与 `apply_hsm`。本文区分已经实现的行为和尚未优化的成本；不是另一套计划中的数据库方案。

## 1. 数据库、CF 与权威边界

每个 Master 使用自己的元数据 RocksDB。现有 CF 为 `inodes`、`edges`、`block`、`location`、`common`。HSM 复用 **common CF**，没有新增 HSM CF，也没有外部文件元数据数据库。

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

下文 `P` 精确表示字节串 `b"\x03hsm:"`。`0x03` 是一个真实的前导字节，不是文本字符 `\\x03`，也不含空格。其后的键采用 UTF-8 文本；HSM value 使用 `serde_json::to_vec` 编码。原生其他 CF 不因此改为 JSON。

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
若 old 存在：Delete(common, candidate_key(old))
Put(common, record_key(new.inode), JSON(new))
Put(common, candidate_key(new), JSON(new.inode_as_string))

若 seal_token 非空且 sealed=false：Put(common, seal:<inode>, JSON(DataKey))
否则：Delete(common, seal:<inode>)

若 garbage 或 native_garbage 非空：Put(common, gc:<inode>, JSON(EntityKey))
否则：Delete(common, gc:<inode>)

由调用方 Commit 一次 WriteBatch
```

所以同一提交批次不会只留下“新 Record + 旧候选键”。状态和 atime 不变时，删除后重新写回的是同一个候选键。Record、候选项及 seal/GC 派生键在稳定投影中对应；派生键可以由 Record 重建，但 Record 中的 pin、回执和 pending 等事实必须来自提交历史，不能丢弃后靠扫描业务目录推测。

这里的原子范围是这些 HSM KV，以及 HSM apply 明确放进同一个 batch 的原生 inode 修改。**不能推广为所有普通写入的 inode 与 HSM 更新都在一个 RocksDB batch**：普通修改调用自己的原生更新和纳管钩子，再写普通 journal；HSM 修改使用 committed HSM transaction。两条路径共享提交串行边界，普通修改返回前等待本节点提交屏障，日志重放执行对应普通钩子。

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
| 原生布局 rename/属性更新 | 更新 path/策略属性与版本 | 调用通用记录维护；排序字段未变可写回同一索引键 |
| 允许的文件删除 | 删除对应 HSM Record | 同批删除对应候选键；有未清理垃圾时先拒绝删除 |

索引不保存文件数据块，也不直接删除 Worker 内容。Release 的原生块撤销、Offline 和持久 GC 事实先提交；物理空间随后由 Worker 删除任务回收。

## 6. 策略如何使用这个索引

1. 从 `policy:<id>` 和 `scan:<id>:<revision>` 读取规则与进度，先判断周期、水位及在途预算。
2. 根据动作选择状态前缀，Release 先 d 再 o。
3. `hsm_scan(prefix, after, limit+1)` 对 common CF 做有界 range_scan。每页**处理最多 256 项**，额外预读 1 项判断是否还有下一页；不是最多发生 256 次物理读取的保证。
4. 从索引 value 解析 inode，再 Get `record:<inode>`，按 inode 解析当前原生路径；之后判断 writing、大小、路径、owner/group、访问/修改/驻留年龄和保护条件。满足访问时间提前结束条件时，转到下一状态范围。
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

completed intent 不参与每轮 Ready/Running 工作索引扫描。当前容量预算仍会分页累加未完成 reserved 项；没有常数成本的聚合计数器承诺。

## 8. 一致性、提交与重放

正常 HSM 事务通过 `JournalEntry::Hsm` 提交。apply 先查原 outcome，避免重复效果，再验证 controller epoch、expected_version、候选 record_checks 及 control 键旧值。成立时在一个 WriteBatch 更新记录、派生索引、必要原生 inode、控制键与 outcome；条件失败保留原始拒绝结果而不修改目标状态。临时 Busy 的重试语义另由动作入口处理。

普通 FUSE 完成路径调用 `hsm_track_file`；正常 CompleteFile 日志在重放时也调用相同钩子。ordinary_format=2 用于新增普通钩子的版本激活，旧日志按旧格式语义解释，不能因为新增回调改变历史 Record.version。

非 testing 启动时，现有实现先清理 common 中的 `P` 投影，再由正常 journal/snapshot 恢复流程重建；普通文件钩子与 HSM apply 共同恢复相同的 Record/索引/任务结果。不能保留未同步的旧 outcome 让它跳过原生 inode 的重放。清理和重建依赖完整提交历史或受支持的 checkpoint，不是从 Worker 块文件猜出归档锚点。

## 9. 当前成本与实施边界

- 每个 Record 对应一个当前状态候选项；每次 atime 保护更新都可能重写整个 JSON Record，并删除/插入候选键，存在写放大。
- 时间索引可减少热记录扫描；路径、owner、大小等仍后过滤，密集不匹配前缀可能跨多轮消耗预算。没有 path×atime 复合索引或按规则物化候选表。
- 当前 Record 将视图、租约、锚点及垃圾集合编码在一个 JSON value，没有拆分为独立关系表；大集合与高频访问尚需压测。
- 当前启动清理会先收集全部 HSM 键到 Vec，再用一个 WriteBatch 删除；其内存和批次大小随 HSM 键数量增长，尚未实现有界分批重建。元数据提交串行边界、日志重放、共享在途预算的扫描成本均需后续性能验证；没有百万文件或生产 HA 承诺。
- 同批更新与日志恢复已经实现；自动对账/修复损坏候选索引的在线运维工具、独立 schema 迁移框架、终态历史自动保留清理并未在此声明完成。

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
