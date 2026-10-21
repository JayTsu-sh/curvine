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

按实测瓶颈排序的 [性能优化方案](optimization-plan.md) 描述自身日志回执、统一提交微批、读取保护和策略扫描改造；状态为设计建议，尚未实现收益。
