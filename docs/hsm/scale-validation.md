# HSM 规模优化与测试记录

日期：2026-10-21。先完成明确问题的修复与 8 项回归，再运行规模测试。性能改动提交 `4b82d089`；可选的隔离预置 fixture 提交 `16885bb4`。测试只使用本地文件归档后端，没有接入带库。

## 1. 本次修复

| 问题 | 已实现的修改 | 保持的语义 |
|---|---|---|
| 启动收集全部键并形成单个大删除批次 | 每批最多 1024 键、256 KiB 编码预算；iterator 在提交前释放；从上批末键后继续 | 全部清理完成后再恢复；普通 inode/挂载/锁记录不被清理；超大键拒绝启动 |
| 每次 Record 修改都删除/重写候选及 outbox | 候选键相同时保留；seal/GC 派生值相同时保留 | Record 和访问修订照常更新；真实 atime/驻留变化仍迁移索引，旧 Release 仍失效 |
| 已可由元数据拒绝的项也先解析路径 | 先检查大小、owner/group、时间、写入和保护条件，再解析当前原生路径 | 路径条件仍看当前 namespace，支持 rename；入队仍经原有 CAS |

实现见 [ordinary.rs](../../curvine-master/src/master/hsm/ordinary.rs)、[lifecycle.rs](../../curvine-master/src/master/hsm/lifecycle.rs)、[rocks_inode_store.rs](../../curvine-master/src/master/meta/store/rocks_inode_store.rs)。没有合并精确 atime，也没有引入外部文件元数据镜像。

8 项回归覆盖批次双上限、清理中断后的继续、保留非 HSM KV、非法上限/超大键拒绝、派生键增删、元数据条件前置与当前路径选择。范围删除 tombstone、额外复合索引和历史结果淘汰未在本次引入。

## 2. 百万 Record 的真实 RocksDB 对比

本节“旧算法”仍包含 HSM，对应此前的实现；原始 Curvine 与 HSM 的前台开销见 [独立性能对比](performance-comparison.md)。

1 万与 10 万各一轮；100 万分别运行优化前/后 **3 轮**，每轮独立临时 RocksDB。大规模每轮有 100 万 Record + 100 万候选键。两组用相同初始化，预置时间不用于表示旧/新写入速度。每种更新执行 5 万次，热集合最多 1024 文件。

构建为 debug test profile，`rustc 1.98.1 (48a229cea 2026-09-01)`，`CXXFLAGS=-g0`，进程可用 CPU affinity 24 个。开发主机有其他服务，无全局清缓存；数字是工程基线，不能当硬件上限或生产 SLO。

下表为百万组的三轮中位数。KV 操作数由 RocksDB sequence number 差值验证，不等于物理写入字节。

| 指标 | 旧算法 | 优化后 |
|---|---:|---:|
| 排序键未变的 5 万次更新 | 250,000 KV 操作 | 50,000 KV 操作（减少 80%） |
| 真实 atime 变化的 5 万次更新 | 250,000 KV 操作 | 150,000 KV 操作（减少 40%） |
| 排序键未变的更新耗时 | 7.42 s | 5.59 s |
| atime 更新耗时 | 7.43 s | 6.60 s |
| 清理 200 万 KV 的最大批次 | 2,000,000 键 | 1024 键，最大约 47.0 KiB |
| 清理阶段采样峰值 RSS | 349.6 MiB | 193.9 MiB |
| 相对清理开始的采样 RSS 增量 | 233.0 MiB | 78.8 MiB |
| 清理耗时 | 14.46 s | 14.92 s |

清理优化主要解决额外应用内存和批次无界问题，**没有把总清理变成常数时间，也没有证明清理更快**。进程 RSS 包含 RocksDB memtable/cache 等，不能把约 47 KiB 的删除批次当作整个数据库的内存用量。

扫描每种条件均实际检查 100 万记录，检查/入选总数一致：

| 条件 | 入选数 | 旧路径回调数 | 新路径回调数 |
|---|---:|---:|---:|
| 大小零匹配 | 0 | 1,000,000 | 0 |
| 大小 0.1% 匹配 | 1,000 | 1,000,000 | 1,000 |
| owner 1% 匹配 | 10,000 | 1,000,000 | 10,000 |
| 仅路径 10% 匹配 | 100,000 | 1,000,000 | 1,000,000 |

这里计数的是策略路径回调，未模拟真实 inode 树遍历。Record point-get 与 JSON 解码仍发生 100 万次，因此不能据此宣称所有扫描快了相同比例。仅路径条件仍是后过滤；复合索引取舍需要另行设计。

原始数据：[metadata.json](scale-results/metadata.json)、[环境与源码指纹](scale-results/environment.json)。旧算法只保留在 test benchmark 中作对照，不是产品兼容路径。

## 3. 百万真实 namespace、策略和恢复

已通过。测试程序：[million_namespace_policy_recall_and_checkpoint_recovery](https://github.com/JayTsu-sh/hsm-rs/blob/main/crates/hsm-curvine/tests/managed_cluster.rs)。机器为同一开发主机；三 Master、单 Worker 位于一个独立 fixture 进程，使用真实 RPC、Raft 和各自 RocksDB/日志目录，HSM controller 是另一个进程。不是三台独立物理机的百万文件验收。

- 通过正常 MasterFilesystem create/complete 钩子预置 **1,000,000 个空文件**，1000 目录、每目录 1000 文件；每 1000 文件等待提交，整体完成后才对父测试发布。另有两个普通 SDK 写入的非空文件，分别为 1,048,593 B 和 2,097,183 B。
- 预置耗时 568.73 秒；随后通过普通 SDK 列目录重新计数为 100 万。内部预置绕过了逐 RPC 的额外往返，不能用这个时间表示普通客户端创建吞吐。
- 零匹配策略实际检查 **1,000,002** 个候选，3,908 页，耗时 139.31 秒；每页 examined ≤ 256，未产生归档或 seal 任务。
- 启用 min_size=1 MiB、指定路径、scan_budget=65536、max_candidates=2 的 Release 策略。控制器穿过百万空文件找到两个非空目标，完成受保护摘要、data-mover 本地搬运、Archive、Release，两个目标变为 Offline。该阶段 137.22 秒，包含完整扫描及数据动作。
- 关闭 Release 策略后，三次普通 SDK 读取触发恰好两个自动 Recall；完整字节和原 DataKey 保持，最终为 Dual，没有显式 Recall 调用或 Recall 策略。
- 等待 follower checkpoint 后强杀整个 fixture，去掉旧 leader 再恢复。启动耗时 66.18 秒，日志确认实际使用 checkpoint；重新 SDK 计数仍为百万，两个目标状态逐字段一致，并可在没有 controller 的情况下重新读取完整内容。
- 恢复时两个存活 Master 的清理日志均验证最多 1024 键、256 KiB 预算。该测试验证此配置的 checkpoint + tail 恢复，不等于任意历史 checkpoint 迁移或完整断电矩阵。

原始结果：[namespace.json](scale-results/namespace.json)。整体耗时 1042.87 秒。

## 4. PVE 集群复测与日志盘

三 Master/三 Worker 仍为 10.131.9.80—82；10.131.9.83 运行 controller、LocalArchive 与读取端 FUSE。Worker 数据仍在各自 64 GiB 本地盘；没有 UFS。原始 A/B 的公开状态在代码更新及日志迁移前后逐字段保持。

预检发现 journal 位于 `pool_50t` 机械盘：12 次 4 KiB 同步写的中位约 94.62 ms。仅为三台测试 Master 各新增 16 GiB `local-lvm` SSD 日志虚拟盘，挂载至 `/var/lib/curvine-hsm/journal-ssd`，保留原日志目录，逐台停服复制、更新配置后恢复。fstab 使用 UUID，Master 单元设置 RequiresMountsFor。三节点同样探测的中位分别约 0.452/0.483/0.451 ms；同步写和提交屏障均保持启用。

原机械盘预检因耗时中止（当时观察到 752 个文件，目标 2000），没有完整同规模吞吐结果。SSD 后新建 **2000 个空文件**，16 并发，耗时 **33.232 秒**，errors=0；再从 FUSE 枚举得到 2000，抽查为 Online、unsealed、length=0。约 60 文件/秒只代表该配置的预填结果，逐 RPC 提交仍有明显吞吐约束；不能用它推断百万普通客户端写入达标。

功能复测另建 `/hsm-scale-pve-20261021/`：通过 .80 FUSE 普通写入 A/B/small/hot/excluded，普通初始两个副本；验证高水位未满足时不入队，降低测试水位后按访问年龄归档并释放 A/B，其他三个保持 Online。三 Worker 实地检查 A/B 的所有 `blk_*` 均已删除，逻辑释放 14,680,112 B、两副本合计 29,360,224 B。

.83 的四次并发 FUSE 普通读（A 三次、B 一次）只产生两个 `__access_recall` Done，SHA256、长度、inode、DataKey 保持；没有显式 Recall 或启用 Recall 策略。快速测试规则最终禁用。原始摘要：[pve.json](scale-results/pve.json)。这轮 PVE 测试不是百万文件预填验收。

## 5. 复现

Curvine 仓库，输出目录须为空：

```sh
CXXFLAGS=-g0 cargo test -p curvine-master --locked --lib hsm -j 6
python3 tools/hsm-metadata-scale.py --output /tmp/hsm-meta-small \
  --sizes 10000 100000 --repeats 1
python3 tools/hsm-metadata-scale.py --output /tmp/hsm-meta-million \
  --sizes 1000000 --repeats 3
```

hsm-rs 仓库，namespace fixture 使用专用新目录；该测试为 ignored，需显式运行：

```sh
HSM_SCALE_FILES=1000000 HSM_SCALE_ROOT=/tmp/hsm-namespace-million \
HSM_SCALE_REPORT=/tmp/hsm-namespace-million.json CXXFLAGS=-g0 \
cargo test -p hsm-curvine --locked --features native-integration \
  --test managed_cluster million_namespace_policy_recall_and_checkpoint_recovery \
  -- --ignored --exact --nocapture
```

`hsm-scale-test` 只为测试开启内部预置，默认服务器构建不启用，没有新增批量写入 RPC。内部预置持有原有 HSM 提交边界，使用正常钩子及 journal，每个批次等待提交；不直接灌入 RocksDB Record 或候选表。

## 6. 验收范围

本次区分：存储算法基准、隔离真实 namespace 验证和 PVE 普通 I/O 复测。不能写成“百万非空文件全部迁移/召回”或“百万普通 FUSE 写入性能验收通过”。真实 atime、路径后过滤、全量 namespace 内存、历史数量和提交串行化仍有成本；没有生产 HA 或任意断电/坏盘矩阵结论。
