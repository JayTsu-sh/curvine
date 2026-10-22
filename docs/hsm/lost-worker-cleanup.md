# 失联 Worker 的分页清理

本次改造针对后台清理连续占用提交资格的问题，不改变 HSM 文件权威记录、策略或任务格式。此前 heartbeat 清理先读取整个 Worker 的反向位置索引，再删除全部位置、使 UFS 缓存失效、等待 journal 确认；整个过程持有独占 authority gate。RocksDB 内部虽然按批删除，上层锁仍覆盖整个 Worker。

## 执行流程

1. HeartbeatChecker 将 Worker 标为失联，在同一个 WorkerManager 写锁内取得本次失联的本地 token，并记录当前 Leader 任期。
2. 清理线程进入 Background 资格队列。取得独占资格后，重新检查 active Leader、原始任期和失联 token；后两者改变时不继续删除。
3. 持有 `fs_dir` 写锁，再取得 WorkerManager 读锁，遵循原有锁顺序。从 `location` CF 的 `(worker_id, block_id)` 前缀读取最多 **1024** 项；不填充 RocksDB block cache，不读取整个 Worker 的清单。
4. 一个 WriteBatch 同时删除这页的正向 `(block_id, worker_id)` 和反向位置记录。游标是上一页最后的原始 block ID，按大端字节顺序 seek 并排除该键，兼容有符号 ID 的字节排序和最大的 Worker 前缀。
5. 检查这一页涉及的文件。普通 Curvine 文件保留正常副本修复；UFS 缓存如果丢失完整副本，则保留 UFS 源元数据并使缓存失效。判断时排除该失联 Worker 的**全部**位置，包括尚未处理的后续页。已经在前页失效的缓存，其后续页也不会进入普通副本修复。
6. 释放 namespace 和 WorkerManager 锁；如果发生 UFS namespace 变更，继续在独占资格下确认已有 journal 前缀。确认后安排存活 Worker 的旧缓存删除，然后释放 authority gate。
7. 在资格外，将本页需要修复的块交给副本管理器，再重新排队处理下一页。前台写入、读保护及 Recall 可以在页间取得资格。

普通 Curvine 位置是 Worker 报告派生的事实。没有 namespace 修改的一页既不提交新日志，也不等待空 journal 回执；没有缓存失效时也不写空的 inode WriteBatch。发生 UFS 失效时确认已有日志前缀；发生可能带部分修改的错误时仍保守确认，保留提交不确定时的故障退出保护。

生产 heartbeat 路径逐页交接，不再保留两个与整个 Worker 块数成比例的预备清单。公开诊断方法 `delete_locations()` 仍返回完整清理结果，因此调用者选择该接口时仍承担结果聚合内存。

## 重连、上报和换主

失联 token 是每次失联新建的 `Arc` 身份，不使用毫秒时间戳或可重复的 Worker session 字符串。Worker 再次失联会获得不同 token，旧任务不会误认为是同一次失联。

接受 Start 或 Running 心跳时取消旧 token。已失联 Worker 的 End 心跳不提供新位置，因此保留旧 token，让已有清理继续。新位置报告在 `fs_dir` 锁内取消旧 token 并发布位置；因此，即使 Running 心跳还没到达，旧清理也不能在下一页删除刚上报的位置。失败的 cluster 校验不取消 token。每页的 token 检查与位置删除由 WorkerManager 读锁保护，注册不能穿过二者之间。

token 只是本地后台任务的取消资格，不是文件权威事实，不写入 RocksDB、Raft 或外部元数据镜像。它与 Leader 原始任期检查共同阻止旧本地任务继续执行。角色变化不能撤销已经提交的前缀；UFS 旧式 speculative 修改提交不确定时仍保留原有故障退出保护，不能以“当前已是 Follower”为由放行。

## 范围和限制

- 1024 限制的是每页位置记录数，不是严格的执行时间或文件总块数上限。一个 UFS 缓存文件的完整可读性检查和存活副本删除清单仍可能随该文件块数增长；普通 inode 解码也保留原有布局大小成本。
- UFS 缓存失效仍使用旧式 namespace journal 和独占资格，本次将其限制到每页触及的文件，没有迁移成新的 owned 条件命令。
- 旧缓存的 Worker 删除安排在确认之后、释放资格之前，但仍不是完整持久 outbox；提交后进程退出导致删除安排遗漏的恢复缺口需要后续改造。
- 完整块报告 reconciliation 仍有整个 Worker 的集合/清单成本，未与本次 heartbeat 清理混为同一个路径。
- 副本管理器仍保存必要的修复需求；逐页交接不意味着修复队列总内存与失联块数无关。
- 清理调度和修复需求仍含本地状态。本次阻止旧任务跨任期继续，尚未增加持久清理游标或换主后的自动接续；页间取消测试不能当作完整故障修复验收。
- 本地 token 不代替 Worker fencing、数据副本冗余、网络分区测试或控制器自动接管。

本次没有修改持久格式，仍沿用此前 `hsm_native_v8` 栅栏和匹配版本 Master 的部署要求。百万规模验收、PVE 更新和最终原版 Curvine 性能对比不属于本次回归结论。

## 验证

验证覆盖字节序与前缀边界、逐页双向删除、旧 token 与重连/再次失联、页间任期变化、Running 心跳之前的新位置上报，以及跨页 UFS 缓存判定。真实三 Master/一 Worker 测试会暂停分页清理，再通过 SDK 完成普通文件写入、关闭、读取和关闭读取保护，验证未完成清理时仍可提交前台操作。

该集群测试的失联 inventory 是注入的派生位置记录；前台读写使用真实 Worker 数据。这不是物理磁盘故障或真实 Worker 进程失联的完整 HA 验收。

最终 Source 回归命令：

```bash
CARGO_TARGET_DIR=/work/cargo-target CXXFLAGS=-g0 cargo test \
  -p curvine-master -p curvine-server --features hsm-scale-test \
  --lib --test hsm_optimization_test --test master_fs_test --test inode_test -j 6
```

结果：**290 项 Master、2 项真实 TCP/Raft、64 项文件系统和 6 项 inode 测试通过，0 失败**；2 项 Master 规模测试及 1 项手工文件系统基准保持 ignored。纯位置清理专项用例启用 HSM gate，但使用无法提供提交回执的测试 writer；两页清理成功，证明其不依赖无日志确认。真实集群在首个 1024 项页面后暂停，剩余 1026 项未删除时仍完成 SDK 数据写入、关闭、读取及读取关闭；恢复后 2050 项全部清理，页大小为 1024/1024/2。

最终日志：开发环境 `/work/jay/hsm/research/hsm-worker-cleanup-end-final.log`。前一版完整回归为 `research/hsm-worker-cleanup-root.log`，最终新增的无日志确认优化以 final 日志为准。


配套固定 Git 回归：Source 代码为 `a1878b22f7fdbb5cb4cad532c520015912a511e8`，hsm-rs 提交为 `928492184c7da4491717b29d59aa2d560842c68d`。8 个直接依赖及 29 个 Curvine 锁定包均使用该 Source，其他依赖保持不变，没有 path patch。完整 workspace 启用 `hsm-curvine/native-integration` 后 **32 项功能、四个 crate 的文档测试全部通过，0 失败、3 ignored**；七个真实 managed 场景通过（88.58 秒），单独 native 搬运场景通过（22.79 秒）。包含普通写入/策略 Release/自动 Recall、冷恢复、后台暂停期间 Recall 及独立进程两次在线选主；该组合结果不代替物理 Worker 故障或清理自动接续验收。原始日志：`research/hsm-worker-cleanup-end-fixed-git.log`，详细范围见 [hsm-rs 验证记录](https://github.com/JayTsu-sh/hsm-rs/blob/main/docs/VALIDATION.md#2026-10-22-失联-worker-分页清理与固定来源回归)。
