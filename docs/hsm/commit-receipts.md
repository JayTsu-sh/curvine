# 普通元数据提交回执改造

本轮实现优化方案 P1 的存储侧提交等待，并补充任期约束。SDK 关闭路径缓存、普通请求微批、统一 committed 状态机和读取保护合批仍属于后续阶段。

## 请求如何返回成功

HSM 启用时，普通 create/complete/rename/resize 等修改仍取得 `hsm_mutation_gate`。同步业务处理把本次请求产生的全部原生日志排入同一个 FIFO 队列，随后排入一个只存在于内存的 `Commit` 标记。发送线程遇到标记时刷新此前尚未提交的批次，并在真实 Raft proposal 返回非零 `applied_index` 后唤醒请求。

```text
取得现有 gate，重新确认 Active
  → 固定本次同步修改的 originating term
  → 原生 inode / HSM 投影修改；排入一条或多条原生日志
  → 排入内存 Commit 标记
  → 刷新队列前缀，等待多数派提交和本地 apply 确认
  → 返回原业务结果，释放 gate
```

如果日志已经因批大小或定时刷新而完成提交，标记直接确认，不另发 proposal。没有产生新日志的请求也不会仅为等待而生成空 journal。前缀可能包含同期后台入队的日志，这是保守等待；不是按不保证入队顺序的 `op_id` 推测完成。

原路径普通日志提交后还要追加一个空 `HsmEntry`，再等它提交和 apply。现在普通修改移除这条持久化 barrier。HSM 控制操作自身的读 barrier、条件事务、结果指纹校验和读取租约保持原语义。

`JournalMessage` 是发送队列的运行时封装，不参与序列化。持久化的 `JournalBatch` / `JournalEntry` 编码不变，旧 checkpoint 和原生日志仍由原有兼容解码路径恢复。

## 换主时如何约束待提交日志

开启 HSM 的 JournalWriter 读取本节点发布的 Raft term；普通同步 RPC 入口固定 term，作用域内所有日志都使用这个值。即使计算过程中 term 更新，也不把请求的后半段标成新任期。作用域使用线程局部 RAII，仅覆盖同步业务处理，不跨 await；不同 writer 分别识别，退出或 unwind 时恢复上层作用域。

提交客户端向本节点发送一次 proposal，不使用会自动切换 leader 的 `leader_rpc`。请求携带可选的 `expected_term`，Raft 节点在加入日志前检查实际 term，拒绝 0、过去或未来任期。由此避免发送线程把旧节点已经推演过的 namespace 日志转发给新 leader，或在同一节点再次当选后提交旧任期的队列内容。批次不会混合不同 originating term。

收到非零 committed/apply index 的匹配 proposal 才确认等待者。缺少 index、连接错误或任期拒绝都按结果不确定处理；发送线程沿用现有失败即终止 Master 的方式，交给进程管理重启和已提交日志恢复。此处选择保守失败，不在原生 speculative namespace 上自动重放未知结果。

已确认的原操作结果可以跨任期成立；任期约束针对旧待提交日志，不把已经提交的成功改判为失败。客户端等待超时、取消或 ACK 丢失不撤销已经入队的操作。取消等待者后发送线程仍提交队列前缀，下一等待者也不能复用旧 ACK 跳过自己的新增日志。

## 可观测性

- `journal_commit_fences`：发送线程消费的内存提交标记数量。
- `journal_hsm_barriers`：提交尝试包含的空 HSM entry 数量。
- `journal_flush_count` / `journal_flush_time`：现有真实 proposal 刷新计数与耗时。

性能示例在启用 `hsm-scale-test` 后为每个测量区间保存上述计数的前后值。普通 create/complete 预期有提交 fence，空 HSM barrier 增量为 0；open/close 仍可能因原有读取保护命令产生 barrier，不能宣称整个 HSM 已经无 barrier。

## 验收记录

2026-10-21 对 `826a2ae9` 的代码完成如下验收：

- Master journal 单元测试 26 通过，包含多条原生日志共用一次提交、缺失 index 拒绝确认、取消等待者不跳过下一批、ACK 丢失不自动重发；Raft 实节点测试 2 通过，覆盖旧 term 换主拒绝和多数派提交、本地 apply 前不发回执。
- Master HSM/恢复测试 8 通过、1 项独立规模基准 ignored。固定 Git rev 的 hsm-rs 全工作区 native-integration 测试 30 通过、2 项 ignored；其中 5 项是真实多进程集群测试，ignored 项为子进程 fixture 和需显式运行的百万规模测试。
- 显式百万 namespace 测试 1 通过：内部批量预置 1,000,000 个普通 create/complete 文件；零匹配策略扫描检查 1,000,002 项、3908 页；两个非空文件经 LocalArchive Release，再由 3 次普通 SDK 读取自动合并为 2 次 Recall。排除旧 leader 的重启后，从 checkpoint 恢复并核对文件身份、驻留和数据；恢复清理单批最多 1024 键。原始结果见 [million-namespace.json](commit-receipt-results/million-namespace.json)。该测试不是百万普通客户端写入吞吐测试。

同机隔离运行的 release 基准及每操作 journal 计数见 [改造前后性能结果](commit-receipt-results.md)。基准对照与上述百万测试不并行运行；百万测试曾与 Git 依赖编译重叠，因此其耗时不用于性能改进结论。

## 当前范围与 HA 限制

本轮不移除全局 gate，也没有引入跨普通请求的微批。因此小文件仍受串行提交上限约束；这是减少冗余提交的一步。

普通 namespace RPC 尚未增加跨进程持久化的逻辑请求结果去重。它们遇到结果不确定时仍遵循原有 API 的重试/查询限制，不能套用 HSM 动作已有的持久 outcome 保证。控制器自动选主、后端副本、Recall 私有布局多副本及 Worker 端旧能力撤销仍按 [HA 设计](ha-design.md) 单独实施和验收。现有 LocalArchive 单节点测试不代表端到端容灾验收。
