# Worker 清理的持久接续与修复需求

当前生产 HSM 路径在 Curvine 的复制元数据内保存清理检查点和修复需求。它不维护外部文件镜像，也不改变 hsm-rs / data-mover 的接口。普通文件读写不因这项改造增加日志；记录只在失联清理和修复期间产生。

[分页清理](lost-worker-cleanup.md)解决一次清理长期占锁的问题。本次继续处理两处中断：删除了位置但还没确认游标，以及需求已交给内存副本队列但 Master 随后失联。

## RocksDB 中保存什么

下列键均在 `common` CF，沿用已有 `\x03hsm:` 范围、Raft journal、checkpoint 和尾日志恢复。

| 键 | 值与作用 |
|---|---|
| `\x03hsm:worker-loss:v1:{worker:08x}` | JSON：`worker`、32 位十六进制 `loss`、`term`、`cursor`、最多 1024 项 `pending`、`rewind`；记录检查点和删除前已提交的页面 |
| `\x03hsm:worker-repair:v1:{block:016x}` | JSON：`block`、32 位十六进制 `observation`；保存未完成修复的需求。每次重新暂存页面都生成新 observation，防止旧确认删除新需求 |

`cursor` 是 block ID，扫描按大端键的无符号字节顺序前进，不能按有符号数比较。`pending` 在真正删除之前提交；位置已经删掉也不能据此丢弃 pending。修复记录独立于清理 job，job 扫描完成后仍可保留修复需求。

局部 `Arc` token、每 Worker 的 running 标记、接收联系的单调时刻和扫描器游标仍在内存。持久 `loss` 用于关联工作，不是可以直接删除数据的 Worker capability；每个新任期都必须重新取得本地失联资格。

## 每页如何执行

1. HeartbeatChecker 先检查当前任期的恢复等待窗口。窗口长度使用已有 `worker_lost_interval`，从本进程观察到新 Leader 任期开始计时；每个新任期重新等待。
2. 普通心跳超时把 Worker 移入 Lost。刚收到位置报告等联系时仍需重新等待完整失联窗口。每次接入取得本地 token，running RAII guard 保证同一 token 不重复执行；队列满时非阻塞退让，失败会释放 guard。
3. 在 Background 共享资格下，通过 ReadIndex 和 originating term 检查后提交 job。namespace 原始值和行原始值都是 CAS 条件；不持有 `fs_dir` 锁等待 Raft。
4. 从 Worker 的反向位置索引读取最多 1024 个 ID。**同一 WriteBatch 提交 pending 页面和这一页全部修复需求**，得到 committed/local-applied 确认后才进入位置删除。
5. 进入有界 Background 独占资格，在 `fs_dir → WorkerManager` 的锁顺序下重新检查原始任期与 token。两向位置索引同批删除；UFS 缓存仍按当前 inode 与整个失联 Worker 的不可用状态判断。纯位置删除阶段不等待空回执；UFS namespace 变更或可能已有部分修改的错误仍确认已有日志。
6. 释放该页资格，再用共享条件提交清空 pending、推进 cursor。这里中断时，持久 pending 保留，下一次可以重复执行相同位置删除。Worker 重新出现后旧 token 失效，持久 pending 本身不能使旧删除继续。
7. 每个执行轮次最多 32 个状态推进 turn，然后返回调度器；这不是严格的单文件耗时上限。扫描到尾部条件删除 job，修复需求另行处理。

因此每页会提交真实的恢复记录，Raft 等待使用共享资格；普通位置处理不再等待没有日志内容的确认。它不意味着故障清理可以完全不写日志。

## 换主后如何自动发现和接续

恢复扫描器每轮最多读取 8 个 job，并发现最多 8 个有位置的 Worker。发现 Worker 使用反向索引前缀 seek，一次跳过一个 Worker 的全部块清单，不逐块枚举 Worker ID。调度器还有独立 busy RAII guard，队列满或任务结束后可以再次进入。

两条发现路径都需要：当前任期恢复窗口已过、Worker 没有当前注册、最近联系也已经超时。已在本任期重新注册并有新联系的 Worker 会使旧 job 取消；独立修复需求仍需检查当前状态。

持久 job 的 term 与当前任期不同，或本地确认了新一轮失联时，先保留并重新暂存 pending，然后重置 cursor、设置 rewind。完成 pending 后，从新 Leader 的本地位置清单重新扫描。位置来自 Worker 上报，各节点及恢复后的投影可能不同，不能直接把旧节点的 cursor 当作新节点完整清理的证明。

即使第一次调度在 job 提交前失败，只要位置事实存在，Worker 前缀发现也会重新安排清理。已经删掉所有位置但尚未确认的页面则通过持久 job 被发现。Follower 只应用捕获的 CAS 记录，不依据自己的 WorkerManager 或位置清单执行删除。

## 修复需求何时可以删除

每轮最多读取 64 条修复记录；尚不健康的块重复交给现有 MasterReplicationManager，其自身继续去重和重试。**放进内存执行队列不算完成**，需求仍保留在 Raft / RocksDB。

满足以下之一才准备确认：当前 inode 已不存在、该块已不属于当前文件，或当前布局中本任期有新联系的 Live Worker 数量达到当前副本策略。旧任期留下的 Live 标记和旧位置不能单独作为健康依据。这个判断使用当前的存储元数据与 Worker 上报，不升级为独立的物理副本证明。

确认提交同时比较 namespace、修复记录的原始值和当前 inode 的原始字节。新一次失联改变 observation，或文件身份/副本策略在准备后变化，旧确认不发布。一次确认批次最多检查 64 个 inode；失败仍保留需求供后续重试。

## 日志格式与兼容性

内部 envelope 为 `hsm-worker-cleanup-v1\n` 加 JSON，不是新的客户端 RPC。命令最多 1025 个工作行、64 个 inode 检查和 2 MiB payload；写入仅限上述两个前缀，检查重复键、键和值的身份、token 格式及 pending 顺序。所有条件检查通过后才写一个 RocksDB WriteBatch。过期命令返回 `applied=false`，不写工作流 binding/outcome；重试依据持久 job 和行 CAS。

新增空 CF `hsm_native_v9` 作为 checkpoint 格式栅栏。全部 Master 必须使用匹配版本，不能滚动启用新日志。旧 v8 opener 拒绝包含 v9 的 checkpoint；已有 v2 至 v8 的日志解释保持。

## 验证与剩余范围

Source 回归已通过：**297 项 Master、2 项真实 TCP/Raft、64 项文件系统和 6 项 inode 测试，0 失败**。规模与手工基准保持 ignored。最终日志：开发环境 `research/hsm-cleanup-recovery-root-final.log`，相对 `/work/jay/hsm`。

新增测试覆盖 pending 与需求同批发布、两份投影重放、删除后检查点仍保留、新位置阻止旧页面删除、新 observation 阻止旧确认、namespace 与副本策略变化拒绝旧确认、最大 payload、恢复等待窗口、队列满后的 guard 释放、稠密 Worker 前缀发现及 v9 checkpoint 栅栏。

当前仍有以下边界：

- UFS namespace 失效和旧缓存 Worker 删除仍使用旧路径；本次持久修复需求不是完整的旧缓存删除 outbox，不能声称覆盖所有 UFS 删除副作用。
- 一个大 UFS 文件的解码和完整可读性检查仍随其块数增长，完整块报告 reconciliation 仍有整份清单成本。
- 此处修复需求持久化不代替 Worker 数据协议 fencing、物理磁盘冗余、私有 Recall 多副本或归档后端 HA。
- 新版百万持续负载、最终原版 Curvine 对比及 PVE 更新尚未执行。
