# Curvine HSM 设计

状态：已实现 v0.6，2026-10-21 发布。普通 FUSE 文件自动纳管、容量/访问年龄策略、周期索引扫描和普通读取自动 Recall 已实现，并在三 Master/三 Worker 的 PVE 集群验证；普通初始两副本、无 UFS，带库后置。详见 [实现状态与限制](adapter-status.md)与[生命周期实现](lifecycle.md)。发布分支为 Jay fork 的 main；hsm-rs 使用 Git rev 与 Cargo.lock 固定配套版本。

目标是在 Curvine 内保存权威事实、生命周期策略和受保护的数据效果，由外部 hsm-rs 协调 Archive / Release / Recall；data-mover-rs 内置 Tape Adapter 并依赖 tape-rs 客户端，tape-rs 服务负责实际带库及其他介质相关工作。

| 文档 | 内容 |
|---|---|
| [RocksDB 记录与候选索引](rocksdb-records.md) | common CF 键值编码、Record JSON、索引排序、WriteBatch、扫描与提交重放 |
| [生命周期实现](lifecycle.md) | 原生策略、有界候选批次、持久任务、claim/finish 与恢复 |
| [Adapter 实施状态](adapter-status.md) | 已实现闭环、提交/读取保护、配置与限制 |
| [详细设计](design.md) | 决策、职责、权威记录、版本、稳定视图、策略、访问、容量和 GC |
| [Interface 与操作协议](interfaces.md) | 命令封装、错误、幂等、Archive / Release / Recall、Worker fencing |
| [实施与验收](implementation-plan.md) | 源码落点、依赖顺序、故障矩阵、升级与发布门槛 |

首个对接实现见 [hsm-rs 通用分支](https://github.com/JayTsu-sh/hsm-rs/tree/feat/generic-hsm-interface)。hsm-rs 直接替换旧专有实现，不保留 Lustre/copytool/v1 插件兼容路径；以 ManagedStorage / ArchiveBackend / DataMover 为唯一核心 Interface。本地 reference Adapter 的单主机保证不等于 Curvine 生产权威保证。

仓库：[JayTsu-sh/curvine](https://github.com/JayTsu-sh/curvine)，fork 自 [CurvineIO/curvine](https://github.com/CurvineIO/curvine)。发布分支：`main`；开发分支：`feat/hsm-adapter`；早期设计保留在 `docs/hsm-design`。

## 源码基线

- 原始深入调查：`964b98f6a36e12230b97e07ef871b918eb5a73d5`。
- 本设计分支的代码基线：`feb3c9e20617d5c3f36c67d44033239b6ac92470`。
- 两者相差一个提交：多块位置查询改用有界 RocksDB iterator，缩短 Worker 状态锁持有范围，并增加对应测试。已静态核对四个变更文件；该差异未增加 HSM 权威提交、稳定版本视图或 Recall 发布能力。
- 设计阶段只读源码和核对文档；后续新增控制入口与编译/集群验证的范围见 [Adapter 实施状态](adapter-status.md)。未访问带库。

除实施状态列明的已实现功能外，设计中的新增类型、字段、模块、配置及方法均为目标设计。文中的状态表是需要实施的语义约束；具体 Rust/protobuf 定义在实现时按本基线验证。

## 固定决策

1. 不维护外部全量 namespace / 文件属性镜像；策略直接查询 Curvine 原生记录。
2. HSM 工作流检查点与正确性记录保存在 Curvine 的复制权威域；外部核心拥有工作流逻辑。
3. 生命周期筛选在 Master 后台；复杂重试、后端选择、对账由 hsm-rs 承担。
4. 第一版只归档已完成文件，采用持久 freeze/pin；受管范围禁用短路数据访问，使用新协议客户端。
5. Release 与归档分开；Recall 以 `CommitRecall` 成功为完成，空间物理回收另行观察。
6. 先证明 committed command/apply，再启用唯一在线副本释放。
7. 磁带只是一个后端；存储记录稳定 ArchiveKey，不解释条码、extent 或 repack 位置。
8. 对接优先：本地 managed source + 本地 archive + 真实 data-mover → Curvine Adapter → 自动生命周期与完整故障验收 → 带库。

第一版限制及未来路线见 [设计范围](design.md#2-第一版范围与明确限制)。

## 规模优化与验收

2026-10-21 增加有界启动清理、差量派生索引维护和元数据条件前置。百万 Record 三轮对比、隔离 MiniCluster 的百万 namespace/策略/召回/checkpoint 恢复，以及 PVE FUSE 复测的范围与原始数据见 [规模测试记录](scale-validation.md)。百万 namespace 通过内部正常钩子批量预置，不代表百万普通客户端写入吞吐验收。

原版 Curvine、HSM 关闭、HSM 开启的前台性能差异、策略扫描影响与复现方法，另见 [引入 HSM 的性能对比](performance-comparison.md)。这与前文“已有 HSM 的优化前后”属于不同对比。

按实测瓶颈排序的 [性能优化方案](optimization-plan.md) 描述自身日志回执、统一提交微批、读取保护和策略扫描改造。[普通日志回执](commit-receipts.md) 已完成 P1 改造及功能验收；[同语义前后性能对照](commit-receipt-results.md)记录单独基准。P2 的[普通请求过渡微批](mutation-microbatch.md)已落地，[P1/P2 性能与闭环验收](mutation-microbatch-results.md)记录当时的连接池条件、实际 Raft flush 和读侧成本。以上性能数字属于历史提交，不能代表当前代码。

2026-10-22 的当前代码已将 mkdir/create/overwrite/add-block/flush/close/write-open、非递归 SetAttr/Delete 接入 Native owned 命令和原始结果去重；读取保护合批、访问 overlay、有界历史、后台快照筛查及 Worker 网络等待解耦也已实现。Master 260 项及两个真实集群用例通过。rename/交换/替换、递归 SetAttr/Delete 和无挂载 TTL Delete 已使用紧凑 committed 输入与持久清理；各业务共用有界 ReadIndex 资格队列，详见[目录与后台命令](native-namespace.md)。owned 路径共享准备，相邻 Native/HSM entry 可合并 proposal；其余稀有 RPC、完整业务优先级与组合故障矩阵仍在收敛，旧式修改的独占边界尚未删除。完整优化后再执行本轮百万验收与原版对比，进度以[性能优化方案](optimization-plan.md)为准。

[HA 设计与验收矩阵](ha-design.md) 将换主、微批幂等、读租约、控制器/执行器接管、归档冗余及 Recall 布局故障恢复作为性能优化前置约束；P1 已覆盖普通日志 originating term 约束，其余仍分阶段实现。


最新批量/调度改造见 [批量写入与前后台调度](native-batches-scheduling.md)：三个公开批量接口使用单一持久身份与原子成功前缀；目录命令共享等待；Recall/前台/后台分别接入、按 4:8:1 分配资格。Master 268 项及两个真实集群用例通过，含提交后回执丢失注入；固定 Git 与组合 HA 验证仍以优化方案最新追加记录为准。历史段落中的“完整优先级未实现”描述的是当时提交。


固定 Git `222a3254` 的组合验证已通过：Master **270 项**、两个 TCP/Raft 优化用例、完整 hsm-rs **32 项功能及全部文档测试**。新增真实常驻 worker 后台暂停期间普通读独立 Recall、恢复后 Archive；独立 Master 进程两次在线选主/旧任期恢复也通过。当前剩余普通 HSM 控制并行化、旧式后台副作用及规模/最终性能验收，详见[最新进度与边界](optimization-plan.md#当前剩余边界)。未升级 PVE。


文件级 HSM 控制、Seal 和已有 namespace 的 SDK 绑定已改为共享等待；统一 Native/Import 的 inode 预留，以新条件封装区分可重试的过期计划与持久业务拒绝。**276 项 Master 与两个真实 TCP/Raft 用例通过**，见[文件级条件提交](conditional-controls.md)。策略/任务的跨记录条件和少数旧后台路径仍在收敛，尚未执行本轮百万及最终性能对照。


配套 hsm-rs 已固定生产代码 `ad3391bd` 并完成完整回归：**32 项功能与全部文档测试通过**，包括冷恢复、两次在线选主和后台暂停时自动 Recall。Root 的 276 项 Master 与两个 TCP/Raft 用例也通过；固定来源、原始日志和剩余工作见[组合验证](optimization-plan.md#条件提交的固定-git-组合验证代码-ad3391bd)。


后续生命周期优化已将策略更新/扫描入队、任务 claim/finish 和 GC 接入共享条件提交；候选路径/副本数、动作结果和逻辑容量预算均有提交检查，Recall 认领/完成也按实际任务动作进入独立类别。**283 项 Master 与两个 TCP/Raft 用例通过**；资源边界和当前剩余项见[生命周期共享提交](lifecycle-shared-commit.md)及[优化方案](optimization-plan.md)。完整固定来源验证另记，未运行本轮百万或最终性能对照。


生命周期生产代码 `d5e88f1d` 的固定来源组合验证已通过：**283 项 Master、两个真实 TCP/Raft 用例、32 项 hsm-rs 功能及全部文档测试**。在线 HA 新增策略/Running claim 跨选主、读取后的重选与 Release/Done、再次选主及 Recall 后原 finish 结果不变，见[组合验收](optimization-plan.md#生命周期的固定-git-与在线任务恢复验收代码-d5e88f1d)。仍未执行本轮百万及最终原版性能比较。


失联 Worker 清理已改为最多 1024 项分页、页间释放资格并交接副本修复；原始 Leader 任期与每次失联 token 共同取消旧任务，新位置报告早于 Running 心跳时也受保护。UFS 缓存按整个失联 Worker 不可用判断，旧式失效仍保留页内提交保护。实现、回归范围与剩余成本见[分页清理设计](lost-worker-cleanup.md)。
