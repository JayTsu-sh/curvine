# 普通文件生命周期策略与自动召回

实现 v0.6。Curvine 的普通 FUSE 常规文件在关闭路径纳管，候选筛选、任务和驻留权威记录在原生元数据中；hsm-rs 负责周期调度及外部编排，data-mover-rs 搬运，当前归档后端为本地文件。

底层键空间、Record value、候选索引维护和重放细节见 [RocksDB 实现设计](rocksdb-records.md)。

## 策略和扫描

规则支持路径和排除前缀、逻辑大小、owner、group、访问年龄、修改年龄、驻留时长及容量高低水位。正访问年龄权重按较冷文件优先。高水位启动，低水位停止，在线池内各规则共享未完成任务的预计回收预算。策略 JSON 不兼容 GPFS 策略文本，也不提供任意 SQL 表达式。

候选索引为状态、访问时间和 inode，先处理 Dual 再处理 Online；访问年龄不再符合时可结束该状态范围。每个请求最多处理 256 个候选项，并额外预读 1 项判断下一页。周期、每轮预算、候选上限、持久游标和容量触发锁存共同限制扫描成本；其他条件仍在候选记录上判断。没有递归目录枚举或外部元数据镜像，分页也不是全局 namespace 快照。

入队同时比较策略版本、源记录版本、确定性任务键和实体 active index。公开状态版本、访问时间和驻留区间纳入任务身份，旧观察失效后可以重新选择。Ready/Running 使用独立索引，召回优先于归档。

## 归档和释放

Release 策略可以选择 Online 文件，先取得受保护的内容摘要，外部执行 Archive；后端提交回执后，Curvine 登记锚点并变为 Dual。随后策略为同一文件生成 Release：外部 HSM 验证回执，Curvine 条件撤销在线布局并提交 Offline，持久 GC outbox 再驱动 Worker 删除全部旧普通块副本。DataMover 不直接设置 Offline，也不删除 namespace。

普通块视图的 capability 是紧凑引用，执行器通过持有的 view 权限查询布局，归档提交不把整份块列表重复塞进控制请求。未关闭文件、活动视图、读取保护、缺少锚点或过时观察阻止释放。重写和截断使旧内容锚点失效；读打开、续租及有效关闭更新访问时间和状态版本，旧 Release 任务不能继续使用旧判断。

## 访问召回

更新后的 SDK/FUSE 在读打开时请求受保护读取。Offline 文件原子产生共享 Recall intent，应用等待，外部 worker 从归档搬运到私有布局并校验。Worker 封存后由 Curvine 在原 inode 发布 Dual，再继续读取。同一离线版本合并并发请求，不需要 Recall 策略或用户执行 Recall 命令。

读取租约、原生校验保护、归档视图、私有布局及 GC 都在 Curvine 保存。中断的源校验有独立 outbox，策略关闭后执行器仍可以恢复校验。已有 Running 工作仍通过 controller epoch 接管；不凭超时删除保护。

## 普通元数据与提交恢复

HSM 控制与普通元数据写入共享提交串行边界，普通写入预更新后等待本节点提交屏障，避免预更新与 HSM CAS 的重放结果分叉。普通数据 I/O、心跳和 stat 不使用该提交边界。

namespace 的 ordinary_format=2 版本化新增普通文件钩子；历史记录缺少该字段时按旧规则重放，升级本身通过日志提交。重放普通修改不会使用其他节点的当前时钟重新判断读取租约。重命名、元数据、截断和删除维护对应的 HSM 记录和索引；TTL 不绕过 HSM 生命周期。

## 当前边界与验证

支持非 UFS、单硬链接常规文件。恢复布局支持普通读取和全量覆盖，其追加及完整可变语义尚未覆盖；活动保护可能使修改需要重试。初始普通块遵循 replicas，私有 Recall 布局仍为单副本，不参与普通块修复及完整统计。当前没有硬容量预留、磁带后端或生产 HA 承诺；百万 Record 基准与批量预置 namespace 验证见 [规模测试记录](scale-validation.md)，普通客户端百万文件写入吞吐未验收。

完整设计、策略示例与运行验证在配套 [hsm-rs main](https://github.com/JayTsu-sh/hsm-rs) 的 docs/DESIGN.md、docs/VALIDATION.md、examples/policies/cold-local.json。发布版本使用固定 Git 依赖；联合开发可用 hsm-rs/tools/curvine-workspace-config.py 生成 Cargo path patch。
