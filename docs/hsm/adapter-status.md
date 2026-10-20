# Curvine HSM 闭环实现

2026-10-20。实现分支 `feat/hsm-adapter`；通用协调及执行器接入在 [hsm-rs feat/curvine-adapter](https://github.com/JayTsu-sh/hsm-rs/tree/feat/curvine-adapter)。

## 范围和入口

第一版使用新建、不可变的 `/.hsm/<name>` 文件。首次 HSM 状态请求通过 committed journal 初始化该目录；普通创建、覆盖、追加、截断、硬链接、重命名、删除、递归 free/setattr 等路径不能修改此作用域。现有普通文件不能直接加标记转管；导入会生成新的原生 inode，之后 Archive/Release/Recall 保留该 inode 和 generation。

| 层 | 已实现责任 |
|---|---|
| Master 原生 authority | namespace incarnation、controller epoch、EntityState、pin、私有布局、归档 anchor、工作流绑定、原始成功/确定失败结果、GC outbox |
| HSM 核心 | hsm-rs 的通用 HsmEngine 选择 ArchiveBackend、协调复制/commit、核对后端回执、执行 Archive/Release/Recall；Curvine 不解释介质位置 |
| 执行器 | hsm-curvine CurvineDataMover 解释精确 view/private-layout capability，经真实 data-mover-rs Local transfer 搬运；当前使用本地完整 spool |
| Worker | 文件型 HSM allocation，独立于普通 block ID/短路路径；按 allocation 串行写入、前缀幂等、完整 Verify、fsync + Seal、持久写权限撤销、删除墓碑 |
| 本地归档 | hsm-local LocalArchive 的不可变对象、独立 digest 验证、fsync 回执及精确 inspect；不是源 namespace 镜像 |

数据状态为 `Online → Dual → Offline → Dual`。Archive 不隐式释放；Release 提交撤销 published 引用并记录 GC，随后重试物理删除。Recall 在原 inode 的新 UUID allocation 中写入，Worker 完整校验并封存后，Master 才提交 published 引用；失败保持 Offline。

## 提交与读取

`JournalEntry::Hsm` 追加在原 enum 末尾，保留已有 bincode discriminant。它走原 JournalWriter 队列：先 flush 前面的普通日志，再单独提交 HSM 命令。成功要求非空 applied_index；空成功和网络不确定性不会变成已提交成功。Leader 与 follower 执行同一 native apply，HSM apply 错误禁止被 UFS 错误跳过选项忽略。

权威记录写在原 RocksDB common CF 的 `0x03 hsm:` 内部键空间；记录、原始结果和涉及的 inode/edge 同 batch 写入。元数据 DB 沿用 Curvine 的无 WAL projection；启动时清除此内部 projection，再从已提交日志或 checkpoint 重建，避免残留 outcome 跳过 inode 恢复。此处没有外部数据库或全量文件属性镜像。

状态读取先提交一个空 HSM 屏障，并等待它在当前节点本地 apply。屏障不建立持久 outcome；超时/取消清理本地 waiter。这样新 leader 的查询不会在旧 committed 前缀尚未追平时给出过期 epoch 或错误的“未完成”。

工作流 ID 全局绑定动作种类和 DataKey；步骤键用结构化编码，指纹递归规范化 JSON 对象。相同请求返回原结果，不同动作/输入不能复用 ID。确定条件拒绝也持久化；条件变化后使用新操作 ID。controller claim 的原结果可重查；旧 epoch 的新修改明确 Fenced。

## 启用条件

所有 Master 和 Worker 使用实现本协议的版本，并共享相同的 HSM 启用配置：

```toml
[master]
hsm_enabled = true

[journal]
enable = true
writer_channel_size = 0
leader_create_snapshot = false
```

要求 `testing=false`。初始 leader 快照采用 speculative 元数据路径，因此本版本禁止该路径。正常 journal、无界 writer queue 用于避免 commit-first apply 与普通持锁写入间的队列死锁。启用时在原 meta_dir 内持久保存 `hsm-format-v1` 格式标记；已有 HSM 卷不能关闭 hsm_enabled 后作为普通卷启动。

默认关闭 HSM，原 capability 握手仍全 false。启用后 native_records、stable_views、conditional_release、private_recall、durable_success_results 为 true；配置至少三个 Master 时 distributed_authority 为 true。capacity_reservation 和 transparent_recall 仍为 false；后端 local-fsync 不表示整套存储具有介质/主机故障 HA。

## 数据保护

Worker 只从新的 HSM allocation 起接受 capability，不暴露短路文件名。普通 block RPC、复制和 delete heartbeat 的 i64 block ID 无法寻址此布局，所以旧普通写会话不能修改受管 payload。write/read/admin 凭证分离；执行器不能 Seal 或 Delete。Worker Verify 提供传输证据，Seal 在持锁状态下再次完整校验并同步，之后所有旧写 capability 都被拒绝。

GC 在 committed detach 后使用唯一 allocation ID 和 admin 权限删除；墓碑先同步、payload 后 unlink，不复用 ID。GC ack 使用独立重试身份，CAS/epoch 变化后不会被某次旧拒绝永久卡住。private Recall 可以通过 pending_recall 查询后显式 abort；旧写权限不能重建已删除 allocation。

## 验证与当前限制

真实进程测试启动三个 Master 和一个 Worker，使用独立目录和实际 Raft journal；3 MiB + 19 bytes 完成导入、归档、释放、物理删除与恢复。归档后、释放后、controller epoch 更新后分别 SIGKILL 集群进程并保留目录，重启时排除原 leader，验证原结果、pin、驻留状态、epoch、inode/generation 和全部内容。另覆盖无 anchor、活跃 pin、操作 ID 复用、坏 Recall 不发布、abort 后旧写拒绝与普通访问不能伪造空洞。

运行命令见 [hsm-curvine 文档](https://github.com/JayTsu-sh/hsm-rs/blob/feat/curvine-adapter/crates/hsm-curvine/README.md)。Worker 单元测试验证重开后的 sealed/deleted 权限和坏内容拒绝；旧 journal 兼容测试验证既有 enum 编号未变化。

第一版仍限定：直接子文件、内容不可变、file-backed Worker、本地归档、显式 Recall、attempt epoch 1。普通 SDK/FUSE open 对该作用域明确拒绝，在线数据经 HSM view 读取。已增加原生生命周期规则/候选批次/持久任务与外部轮询 worker，详见 [生命周期实现](lifecycle.md)。尚无访问热度/容量水位策略、透明访问召回、容量预留、普通块统计/副本修复集成、SPDK 或带库；未做断电/盘损坏及完整生产故障矩阵。失败 import 以原输入和操作 ID 重试；失败资源不会按超时自动删除。后续按原设计扩展，不把当前闭环当成全部生产能力完成。
