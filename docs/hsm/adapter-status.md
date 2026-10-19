# Curvine HSM Adapter 实施状态

日期：2026-10-19。代码位于 `feat/hsm-adapter`；客户端位于 [hsm-rs feat/curvine-adapter](https://github.com/JayTsu-sh/hsm-rs/tree/feat/curvine-adapter)。这是接入第一阶段，尚未提供 Curvine 原生 Archive/Release/Recall 工作流。

## 当前代码

| 位置 | 行为 |
|---|---|
| `crates/common/curvine-fs-api/src/rpc_code.rs` | 新增 HsmControl=61，不复用旧操作编号 |
| `crates/common/curvine-proto/proto/master.proto` | 版本化 protobuf envelope，携带有界 JSON 控制体 |
| `crates/common/curvine-model/src/hsm.rs` | 能力发现；版本、格式和大小校验；尚未实现的效果明确返回 Unsupported |
| `curvine-master/src/master/master_handler.rs` | 原生 Master RPC 入口，无新增 inode 或数据布局修改 |
| `crates/client/curvine-client-core/src/file/fs_client.rs` 与 `file/curvine_filesystem.rs` | SDK 原生控制调用，复用现有 Master connector |
| hsm-rs `crates/hsm-curvine` | 通用 ManagedStorage 到控制请求的映射、能力门控、身份/内容/效果结果验证，以及可选原生 SDK 数据读取桥接 |

所有能力目前为 false：native_records、stable_views、conditional_release、private_recall、distributed_authority、durable_success_results、capacity_reservation、transparent_recall。它们不能从配置或普通文件系统能力推导为 true。

实际数据桥接读取普通已完成文件，按块落到本地私有 spool，再由真实 data-mover-rs 执行 Local transfer 和完整 digest 校验。这证明原生客户端和执行器能连接，但不是稳定 DataView，不能据此登记 HSM anchor 或释放源数据。

## 下一阶段的源码约束

后续不能只在本 RPC handler 内添加读写状态：

- HSM proposal 必须与普通 journal writer 队列排序，不能越过尚未提交的 Create/Complete。Leader/follower apply 行为需要统一；只有 committed apply 后才能返回持久效果成功。
- 原生 DB 的内部键空间、typed batch、原始结果和 epoch 必须与 inode 事实处于同一权威提交边界，不能用外部文件或元数据镜像替代。
- 当前普通元数据路径先修改本地再异步 journal。managed scope 必须约束所有相关入口，包含 TTL、内部删除、复制和 Worker 直接数据路径。
- SDK 关闭 short-circuit 只是测试条件。生产 fencing 必须在 Worker 阻止旧 writer、旧 attempt、旧复制任务和被撤销的布局操作；必须考虑已经获得的文件描述符。
- Release 保留逻辑 inode/长度/anchor，仅撤销在线布局；GC 滞后且不得误删新布局。Recall 使用原 inode 私有 layout，seal 后条件发布，不能用普通文件 rename 取代。

## 验证边界

模型协议单元测试验证能力、Unsupported、版本和消息大小。hsm-rs 合同测试使用显式 fixture，仅验证客户端合同。可选 native-integration 使用真实临时单 Master/Worker 与 TCP SDK，验证能力协商、普通读取和 data-mover；该 harness 关闭 journal，不证明复制提交或跨节点 HSM 持久性。

完整实施与故障验收仍遵循 [implementation-plan.md](implementation-plan.md)。当前最先需要实施的是原生 committed record/outcome 和 managed-file 修改保护，之后才启用稳定视图和 Release/Recall。
