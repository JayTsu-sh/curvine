# Curvine HSM 当前实现 v0.6

2026-10-21 发布。Curvine 实现合入 Jay fork 的 main，配套 hsm-rs 合入 main，构建通过 Git rev 与 Cargo.lock 固定依赖。普通 FUSE 文件自动纳管、存储侧周期策略筛选、外部归档与 Release、普通读取自动 Recall 已在 PVE 三 Master/三 Worker 集群通过。带库后置。

## 分层与权威

| 组件 | 当前责任 |
|---|---|
| Curvine Master | 普通 inode/generation 与 HSM 记录、规则、状态/atime 索引、持久扫描游标、任务及原始结果、稳定视图、读取租约、条件 Release 和原 inode Recall 发布 |
| hsm-rs 核心/worker | 周期触发索引扫描、领取任务、Archive/Release/Recall 编排、回执验证、原始效果对账和 GC/中断 seal 恢复 |
| data-mover-rs | 固定普通块视图与本地归档、归档与私有恢复目标之间的实际字节搬运，使用完整 spool |
| Curvine Worker | 普通块读写、旧副本回收、HSM 私有布局 Write/Verify/Seal、持久权限撤销与删除墓碑 |
| LocalArchive | 不可变归档对象、校验、fsync 回执和精确查询；不是源 namespace 镜像 |

权威记录在原 RocksDB common CF 的 `0x03 hsm:` 内部键空间，沿原 journal/Raft 提交，不维护外部文件属性镜像。部署的三个 Master/三个 Worker 各为独立服务进程；普通文件初始两副本，不配置 UFS。外部 .83 单个进程包含核心、执行器库和 LocalArchive。

## 普通写入和策略

非 UFS、单硬链接常规文件通过正常完成日志自动纳管，无需 import。writing 文件不选；策略候选才在保护下计算摘要。原生布局重写/覆盖/截断推进 generation 并清除旧 anchor，重命名更新路径。不可变 `/.hsm/<name>` import 继续作为诊断原语入口。

策略支持容量高/低水位滞回、正访问年龄权重、路径/排除前缀、大小、owner/group、访问/修改/驻留年龄。外部 worker 周期发起，Master 按状态/atime/inode 索引筛选；不递归 readdir。单次最多 256 索引项，scan_budget 与 max_candidates 限制每轮读取和选择，持久游标及 next_due 支持恢复。路径等条件仍后过滤，未做大规模吞吐基准。

Ready/Running 独立工作索引，Recall 优先。任务 ID 绑定规则版本、内容身份、动作阶段及源观察；领取和效果提交重查。读取更新 atime 和公开状态版本，旧 Release 被 Superseded，后续可按新观察重新选择。待回收预算跨规则共享，但不是硬容量预留。

## 数据流水线

```text
普通 FUSE close → Online
  → 策略选择 / 摘要保护 / Archive intent
  → hsm-rs 调 data-mover 复制 → 后端验证并 fsync 回执
  → Curvine 登记 anchor → Dual
  → 后续 Release intent / 重新核验后端
  → Curvine 条件提交 Offline + GC outbox + outcome
  → Worker 回收全部旧普通块副本
```

DataMover 不设置 Offline；Release 是条件存储效果。Done 与物理 GC 分开，回收中断由持久 outbox 继续。

```text
普通 FUSE/SDK open/read Offline
  → Curvine 合并 __access_recall intent，应用等待
  → 外部 worker 领取 / data-mover 恢复私有布局
  → Verify → Worker Seal → Master 原 inode 发布 Dual
  → 等待的读取获得租约并继续
```

无需 Recall 策略或显式命令。SDK 最多等待 300 秒，每 200ms 查询；读取租约 120 秒、每 30 秒续租。私有布局发布前仍 Offline，校验失败不发布半成品。

## 提交、重放和配置

普通元数据修改与 HSM 控制共享提交串行边界；普通预更新后等待本节点 apply 屏障，避免未提交状态与 HSM CAS 交错。正常历史修改重放不按新时钟重新判断旧租约。ordinary_format=2 的钩子通过提交激活，历史日志仍按旧格式语义执行，避免改变历史 record version；PVE 升级前后公开状态逐字段一致。

```toml
[master]
hsm_enabled = true
[journal]
enable = true
writer_channel_size = 0
leader_create_snapshot = false
```

必须 testing=false，匹配版本的 Master/Worker/FUSE/SDK 一起升级。HSM 卷有 hsm-format-v1 标记，不得关闭 HSM 或直接降级。记录沿原 common CF 的日志投影恢复，不引入另一数据库。

启用后 native_records、stable_views、conditional_release、private_recall、durable_success_results、transparent_recall 为 true；三个 Master 配置下 distributed_authority 为 true。capacity_reservation 仍为 false。已管理文件不允许 TTL 绕过生命周期。

## 验证与边界

最终三项普通文件集群测试和两项既有原生集群回归通过；另验证水位滞回及真实条件函数。PVE 普通 FUSE 写入五文件，A/B 策略离线，实际删除全部双副本 29,360,224 字节，热/小/排除文件仍在线。四次并发普通读共享两个 Recall Done，完整 SHA256、inode、DataKey 保持；未调用显式 Recall。源 seal 中断后可以在无启用策略时由 CLI worker 恢复，紧凑 view 关闭后旧能力被拒绝。

[完整部署与流程图](https://github.com/JayTsu-sh/hsm-rs/blob/main/docs/DEPLOYMENT.md)、[完整测试](https://github.com/JayTsu-sh/hsm-rs/blob/main/docs/PVE-FUSE-HSM.md)、[构建与验证](https://github.com/JayTsu-sh/hsm-rs/blob/main/docs/VALIDATION.md)均在配套 hsm-rs。发布构建无需本地路径补丁；多仓库联合开发仍可使用 tools/curvine-workspace-config.py。

Recall 私有布局当前单副本，不参加普通块修复与完整统计；普通读取和全量覆盖可用，恢复布局追加及完整可变 POSIX 尚未覆盖。活动读取/归档保护期间修改可能被拒绝。控制器旧 Running 接管仍需显式 epoch；LocalArchive 单节点、完整 spool、全部 VM 同 PVE。没有带库、部分恢复、完整断电/盘损坏矩阵或生产 HA 承诺。
