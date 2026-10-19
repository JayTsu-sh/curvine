# HSM 实施计划与验收

本计划服务于 [详细设计](design.md) 和 [Interface](interfaces.md)。当前交付是设计文档，以下模块和测试尚未实现；不把静态调查记为测试通过。

## 1. 源码改造映射

路径相对仓库根；“新增”表示目标路径，最终可按项目惯例调整。

| 责任 | 当前落点 | 建议改造 |
|---|---|---|
| 提交与结果 | `crates/metadata/curvine-raft/src/raft/{raft_server,raft_client,raft_node}.rs` | 修正重复 propose；接通 committed/apply ack；read barrier；不得默认空响应成功 |
| journal/apply | `curvine-master/src/master/journal/{entry,journal_writer,sender_task,journal_loader,ufs_loader}.rs` | 版本化 command、leader/follower 同 apply；新旧日志迁移；提交后 outbox |
| 原生事务 | `curvine-master/src/master/meta/{fs_dir.rs,store/inode_store.rs,store/rocks_inode_store.rs}` | typed HSM batch、owner 索引、outcome、applied marker、snapshot/restore |
| inode/分配 | `curvine-master/src/master/meta/{inode/inode_file.rs,inode_id.rs}` | generation/session/layout 引用、可靠 next_seq high-watermark、格式升级 |
| Master Interface | `curvine-master/src/master/{master_handler.rs,fs/master_filesystem.rs}` | 认证、类型化命令、受管 open/read、全体普通 mutation 防旁路 |
| 权威模块（新增） | `curvine-master/src/master/authority/` | command 提交/等待、统一 apply、持久结果、查询一致性、outbox |
| 数据模块（新增） | `curvine-master/src/master/hsm/` | record、view、layout、claim、access、gc、强保护 |
| Lifecycle（新增） | `curvine-master/src/master/lifecycle/` | manager、scanner、rule、capacity、intent |
| TTL/水位 | `curvine-master/src/master/{fs/master_actor.rs,meta/inode/ttl/,quota/}` | 后台接入；受管范围转 intent；按实际回收结算 |
| 协议和模型 | `crates/common/curvine-proto/proto/{master,worker,common}.proto`；`crates/common/curvine-model/src/` | HSM envelope、能力协商、session/receipt、typed errors |
| Worker | `curvine-worker/src/worker/{block/,handler/,replication/}` | 会话隔离、token、durable receipt、owner 对账、恢复准入和条件 GC |
| 本地/SPDK 存储 | `crates/adapters/{curvine-storage-local,curvine-storage-spdk}/src/` | allocation/owner 持久性、finalize durability、extent pin 和安全回收 |
| 客户端 | `crates/client/curvine-client-core/src/{file,block}/` | managed lease、精确 view/stage、离线等待、所有流/批量重试验证 |
| Unified/FUSE/SDK | `crates/client/curvine-unified-fs/`、`curvine-fuse/`、`crates/sdk/` | 禁止受管 fallback/短路绕过、离线错误、等待取消、能力版本 |
| 配置/CLI | `crates/common/curvine-config/`、`curvine-cli/` | 激活门槛、scope 管理、dry-run、操作/GC/claim 诊断 |

不要先引入一个只透传旧 `free/rename/set_attr` 的 HSM Adapter；测试必须经新 Interface 验证真实不变量。

## 2. 依赖顺序与增量交付

每阶段包含实现、相关测试和格式/升级说明；可拆为多次小提交。这里不创建远程 issue 或 PR。

### P0：统一权威提交

1. 添加可控制的复制/应用故障点与 journal 状态诊断，先复现普通 RPC 与 committed ack 的关系。
2. 修复重复 propose：未完成请求合并等待者，已完成返回可验证结果，失败/丢响应不冒充 success。
3. 实现版本化 NativeCommand/HsmCommand、确定性 apply 与持久 MutationOutcome。
4. 原生相关 mutation 迁到同一顺序；移除新路径 leader speculative preapply 与 UFS inline 副作用。
5. 贯通读取 barrier、snapshot/replay/压缩水位、配置/schema 激活门槛。

退出条件：元数据与 outcome 可在主节点每个提交窗口崩溃后恢复；少数派旧 leader 无新授权；未提交命令无物理副作用；leader/follower 状态一致。未通过时禁止继续做唯一副本 Release。

### P1：管理记录、身份和防旁路

1. typed keyspaces、version envelope、CF/索引迁移、backup/restore。
2. EntityKey/DataKey/revisions、独占 writer session 与管理范围；保护普通 xattr/导入路径。
3. ControlGrant、attempt、Operation、pending intent/event 与幂等枚举。
4. 修复/验证 AddBlock replay high-watermark；范围提交分配、溢出拒绝、owner 台账。
5. 所有 free/TTL/覆盖/截断/链接/rename/delete/Worker 丢失与 report 路径纳入 guard。

退出条件：同名重建不串身份；新写准入立即失效旧版本授权；旧核心、旧 worker、旧客户端均不能绕过；allocator 切主不复用。

### P2：冻结版稳定视图

1. 新 managed 协议、禁用受管短路读写、旧会话清退和 scope 激活。
2. Worker incarnation、InstallFence、会话 drain、重启恢复准入。
3. manifest 分块、PREPARING/READY、持久 source pin 与精确读取。
4. CloseDataView 的撤销/drain/解除保护，外部核心接管准备中/关闭中视图。

退出条件：多块读取跨重连、修改竞争、副本切换和 Master/Worker 重启始终固定同一 bytes；任何无法确认的旧 writer 都不能让 view READY。先验收 file layout。

### P3：软件后端 Archive 闭环

1. hsm-rs 抽取通用 storage/backend Interface，保留 Lustre Adapter；去除其作为唯一状态源的本地 DB 假设。
2. data-mover-rs 对齐现有插件 Interface，增加 view source 和可控软件归档后端。
3. 固定 ArchiveKey、精确状态查询、algorithm-tagged digest、receipt/profile、锚点提交。
4. 在无磁带环境注入 Pending/Unknown、已提交丢回包、同 ID 参数冲突，验证接管。

退出条件：后端接受/暂存不产生有效锚点；核心/worker 重启可依存储和后端事实收敛；无 mtime 猜测或 latest 路径替代。

### P4：Release/Recall 最小闭环

1. managed reader lease、Offline access event、SDK/FUSE 等待/取消、同 DataKey Recall 合并。
2. Release、detached owner、持久 GC、精确条件删除和物理回收结算。
3. CapacityClaim、原 inode 私有 RecallLayout、分片 allocation、stage 写入/报告/修复。
4. finalize/整文件验证/SealRecall、CommitRecall 的原子 root 切换与结果。
5. Abort/换 attempt/删除/覆盖并发和 reclaim 对账。

退出条件：原 inode 及硬链接保持；半成品不可见；旧 GC 不删新 Recall；空间耗尽不会错误发布；逻辑 Release 与物理回收可分别观察。

### P5：原生策略与预算

只读 scanner/dry-run 可在 P0 前并行开发；生产自动动作依赖 P0–P4。

1. 规则 envelope、版本激活、轻量 metadata projection、稳定 ID 游标和有界扫描。
2. Archive/Release 分队列、持久去重 intent、周期重扫与过期候选复核。
3. 水位、预归档储备、Recall 保底空间、在途/实际回收结算。
4. 规模测试，测量前台 p50/p99、扫描周期、Raft/outcome/索引增长和预算误差。

退出条件：无需外部全量镜像覆盖目标 namespace；规则更新、扫描重复/漏过、切主和批次重复均不越过最终保护；旧 TTL/eviction 不再旁路。

### P6：Tape Adapter 和第二后端

1. data-mover-rs 依赖 tape-rs 客户端，在内部 Tape Adapter 提供 Archive/Inspect/Read/Retire。
2. tape-rs 完善不可变归档身份、跨进程提交查询、catalog/profile 与 Unknown/Absent 区分。
3. 先协议 mock，再独立安排带库实机测试；设备排他和物理持久性单独验收。
4. 增加第二种实际归档后端，证明 HSM 不依赖条码/装带/repack 字段；多副本和 retirement 闭环。

退出条件：202/STAGED 不授权释放；落带后回包丢失可精确对账；位置变化保持 ArchiveKey；物理删除与逻辑 retire 清楚区分。软件后端通过不等于实际磁带耐久性通过。

## 3. 核心故障矩阵

| 场景 | 检查的不变量 |
|---|---|
| 本地预检后、提案前进程退出 | 没有公开 metadata 改动和外部删除 |
| 提案仍在途时同请求重试 | 不提前 success；原 mutation 最多一个效果 |
| 已提交未 apply、apply 后未回包时崩溃 | 新 leader 重放原结果，inode/outcome/outbox 无部分状态 |
| 少数派旧 leader 接收管理请求 | 不能产生新 grant；负查询不能授权补偿 |
| 日志压缩、snapshot install、恢复再次中断 | allocator、pin、stage、outcome、applied 水位一致 |
| AddBlock 后、CompleteFile 前切主 | allocation seq 不复用，24-bit 耗尽拒绝 wrap |
| 新写获准但客户端尚未写 | generation 已换，旧归档不授权当前 Release |
| 多块读取停在尚未打开后续块时修改 | freeze 拒绝修改；读取不混合版本 |
| 已建立远端/批量 writer 在 fence 后继续写/finalize | 被拒绝并排空，不能改变选定 bytes |
| 旧短路 FD 存在 | scope 激活/READY 被阻止，不能用租约声称已撤销 |
| view PREPARING/READY/CLOSING 中换主、换核心、Worker 重启 | pin/fence 可恢复，不自动解冻或回收 |
| 最后 unlink、目录递归删、另一 hardlink、TTL/free 与 view 竞争 | Busy 或保留规则一致，无路径旁路 |
| Release 与 Open/RenewRead 竞争 | 有读者 Busy，已离线的新读触发 Recall |
| detach 已提交、GC 未做/部分做时崩溃 | 可靠续做；空间按实际确认结算 |
| 旧 GC 在新 Recall/新 generation 后迟到 | 不匹配新 AllocationKey，不删除正式布局 |
| 多读者/预取同时访问离线文件 | 同 DataKey 一次活动 Recall，等待者取消互不破坏 |
| Recall stage 有一块未 finalize 或 digest 不符 | Seal/Commit 拒绝，普通客户端始终看不到 stage |
| stage block report、复制修复、丢节点处理 | 正确认 owner，不当 orphan 删、不修错版本 |
| Recall 期间 rename、覆盖、同名重建、最后 unlink | rename 保持身份；旧 generation 发布被拒绝 |
| CommitRecall 已成功但核心收到 timeout | 查询原成功；Abort 不删除已发布布局 |
| claim 申请/分配/发布/退款各阶段崩溃 | 额度不双花，实际占用不提前退回 free |
| 后端 Pending、Unknown、Absent、Committed 丢回包 | 只有有效 committed 锚点授权 Release |
| retirement 与 Recall/Release 交错 | 被使用/最后必要副本不被删；pending retire 不再计有效 |
| schema 不兼容、关键 replay 解码失败 | 拒绝服务/升级，不跳过推进水位 |
| 灾难恢复时旧集群仍可访问后端 | fencing/隔离完成前不自动 Release/Retire |

每条测试需要断言内容 hash、inode/DataKey、持久 outcome、owner/ref、空间账本和对外可见性，不能只检查 RPC 返回码。Master、Worker 和外部核心均要有真实进程崩溃测试；mock 用于穷举顺序，不能代替物理耐久和数据层故障验收。

## 4. 性能与资源验收

在目标文件数、文件大小分布、块数、副本数和前台并发下记录基线，再测各增量：

- metadata command 提交吞吐/延迟、批次/日志与 snapshot 规模。
- HSM record/outcome/pin/manifest/GC 索引字节及终态保留增长。
- 扫描每页锁占用、全域覆盖时间、前台 p99 干扰。
- freeze 时长分布、阻塞写数量/字节、后端排队对业务的影响。
- Recall 首字节/完成延迟、并发合并效果、claim/物理占用偏差。
- GC 确认延迟、被隔离 Worker 上的空间积压、重启重建时间。

首版不在无实测数据下承诺吞吐或工期。若长期 freeze 不满足业务目标，在正确性基础上选择独立持久 staging 或 COW；不能以删掉 pin/fence 换取性能。

## 5. 发布门槛与回退

发布状态依次为 `DESIGN -> IMPLEMENTED -> SOFTWARE_FAULT_VALIDATED -> MEDIA_VALIDATED -> LIMITED_PRODUCTION`。当前仅 DESIGN。

启用顺序为能力部署、schema/提交模式验证、scope 清退与迁移、dry-run、软件后端归档、显式受控 Release/Recall、自动策略、实际带库。每次扩展 scope/layout/backend profile 都要核实相应验收。

回退首先停新 intent/归档/Release，保留访问 Recall 和已提交 GC 所需兼容读取者；核心接管并 drain 活动任务。已离线数据必须保持可召回，不能直接关闭 HSM 标志使旧客户端把它当空块文件。要退回旧软件，先恢复所有离线文件、清退会话、证明无未完成操作及引用，再执行受审查的格式迁移。

Fork 跟踪上游时单独重核 journal/元数据、block ownership、worker session、Transfer/Unified 及 SDK 差异。上游 merge 不自动提升本设计的验收状态。
