# 已提交的目录操作与后台删除

本页描述 `committed/namespace.rs`、`commands.rs` 与 `background.rs` 的实现。目标是无 UFS 的 HSM 卷；普通写入、策略 Release、自动 Recall 的分层职责不变。

## 两种输入共用原始结果和提交回执

| 输入 | 执行位置 | 工作量与限制 |
|---|---|---|
| mkdir/create/overwrite/add-block/flush/close/write-open、非递归 SetAttr/Delete、Resize/AssignWorker | 协调器准备 owned inode/edge/HSM 变更，apply 条件发布 | 每批最多 32 条、准备预算 2 MiB；投影共享资格 |
| rename/替换/交换、递归 SetAttr/Delete | 协调器提交紧凑输入；每个副本在 committed apply 上展开变更 | 每次一条紧凑命令；共享资格等待提交，工作和原子 WriteBatch 内存仍随子树大小增长 |
| HSM 卷、无挂载时的 TTL Delete | OS 调度线程提交内部条件命令，进入同一个有界 Native 协调器 | 与前台目录命令同样提交，额外核对筛选时的身份和条件 |

紧凑命令保存原始 protobuf、session/sequence/ack、incarnation、请求指纹、操作时间、读取保护时钟及必要的内部 TTL 条件。它不保存展开后的全子树，也不在提交前调用旧式可见修改或发 Worker 删除。apply 先检查去重窗口；已有结果不再展开子树，过期身份不能重新执行。

首次执行时，各副本根据同一已提交前缀生成 owned 变更。inode、目录项、HSM Record/访问 overlay/候选索引、块退休事实以及原始成功或失败结果同一 WriteBatch 提交，随后更新内存树。业务错误在准备阶段丢弃全部变更，不会只 chmod 半棵树或先删除 rename 目标再失败。底层持久化或发布错误继续 fail-stop。

递归操作不会因 2 MiB 的普通准备预算而拒绝一个合法大目录。它仍需要展开实际子树与原子写批；这不是 O(1) 目录操作，也没有据 2,048 文件回归宣称百万子树的内存验收完成。

## rename、属性和删除

rename 保留 inode 与物理块 ID。目录移动和交换转移原有子树，不复制子树再删除原件；遍历后代时加载 FileEntry 对应的真实 inode，同批更新文件的 HSM 路径及版本。交换、覆盖、NO_REPLACE、同 inode 的硬链接、禁止目录成环和跨父目录链接数分别处理。

覆盖目标和最后一个 unlink 都先保存 inode 独立的块退休事实。目录项与 inode 消失后，`gc_batch` 仍能向 Worker 下发删除、处理迟到报告，并在确认后清除事实；逻辑删除成功不等同于 Worker 已释放空间。保留其他硬链接时只减链接数，最后一条链接才退休块。

递归 SetAttr 沿用 Curvine 的 child_opts：后代继承 ACL/副本选项，父目录的 mtime、atime、TTL 和普通 xattr 不向后代传播。ctime 取 Master 捕获的操作时间，不能由客户端伪造。递归 Delete 在准备视图内逐条减链接数，覆盖同一批子树中多条链接指向同一 inode 的情况。

目录摘要不含 children。apply 逐目录范围验证每个实际后代项都在删除集合中，不为每个目录全量遍历删除集合。纯删除在内存树只摘除删除边界并释放原子树，统计按完整目录项集合汇总；避免逐项重建父路径、排序和访问已移除节点。rename 在重新挂接前保留旧子树所有权，必要时按深度移除旧边。

Resize 与后续 AssignWorker 也使用 Native 会话、条件发布与原始结果。截断退休的整块同批建立 GC 事实；保留的尾块按原有 writer 协议调整，写入状态在 complete 之前仍阻止 HSM 归档。Sparse 扩展的未分配块可返回空位置及 alloc_opts，Worker 选择在 Leader 准备阶段捕获，不能在 Follower apply 重新选择。重复分配返回原有位置，不再次占用调度预留；截断再扩展不复用旧块 ID。

## 为什么要记录读取保护时钟

是否存在有效读者不能在每个副本上分别调用当前墙钟，也不能在 follower 上跳过保护。紧凑命令记录接收时的逻辑租约时间和恢复宽限状态；所有副本用该值判断相同的读者到期时间。准备和等待提交期间持有共享投影资格；在 apply 展开时读取当前 Record，因此准备之后才提交的新读者也能阻止整棵子树的 rename、递归属性和删除。旧式先修改投影的 writer 仍被排除。

## TTL 与配额

TTL 候选携带 inode ID 和规范化元数据摘要。apply 同时复核当前路径仍指向该 inode、属性未变、已过期、尚未由 HSM 接管；目录还必须实际为空。候选过时返回空成功，调度器保留仍存在 inode 的后续调度；不能按旧路径删除新建同名文件，也不能删除筛查后才进入目录的后代。

后台线程通过现有运行时派发 owned 任务，等待自身结果，不创建嵌套 runtime。排队、提交和未知结果隔离由 Native 协调器持有；OS 等待超时不取消已经接收的命令。命令仍占用同一个 128 名额的 Native 接收队列。UFS 挂载内的后台操作保留明确的旧式边界。

配额执行放到 blocking 线程，串行消费触发器，避免在 RPC worker 上执行同步扫描/等待。`Ok((false, inode))` 不计回收、不移除候选、不抵扣目标；无进度时本轮停止。原生数据没有 UFS 副本时 TTL Free 不宣称释放，受管文件由 HSM 策略执行 Release。计数表示动作执行成功，并非异步物理 GC 的空间确认。

失联 Worker 的位置清理改为每 1,024 块一个 WriteBatch，同时移除正反两种位置索引；不再为每块单独写一次 DB。位置仍是 Worker 报告派生状态，不能将其当作 HSM 权威记录或已删除物理数据的证明。

## 共享资格调度

`journal/read_cohorts.rs` 将 Native、受保护读取、元数据查询、策略和控制请求的 ReadIndex 需求归入同一个 FIFO。最多 128 个待处理/执行名额，每轮最多 64 个已经就绪的资格等待者；没有固定合批定时器。

在证明开始前先摘出本轮等待者。网络等待期间到达的请求必须参加下一轮，不复用旧证明。证明失败或请求所属任期与证明任期不符时不授予资格。调用方取消不会取消一个仍在服务其他等待者的证明。真正的 prefix flush、quorum ReadIndex 和本地 apply 校验继续由 JournalWriter/Raft 完成。

它统一的是资格证明；各类业务仍保留自己的有界准备队列。共享/独占资格现由 AuthorityGate 按 Recall/前台/后台 4:8:1 调度，Native 队列内部另保证 TTL 的轮次；prepared Native/HSM entry 的 proposal 合并继续由 SenderTask 处理。少数旧式 RPC 的独占保护仍保留，详见[批量写入与前后台调度](native-batches-scheduling.md)。

## 格式与验证

Native journal v1/v2/v3/v4 继续按历史 DTO 解码；v5 是独立紧凑 namespace 输入，公共 NativeMutation envelope 仍为 v1。新增空 CF `hsm_native_v6` 阻止不支持该语义的二进制打开 checkpoint。所有 Master 必须支持后再启用；没有跨版本滚动激活协议。

当前 Master 260 项通过、2 项 ignored；两个 TCP/Raft 优化集群用例通过。覆盖 Native 部分截断/清零/扩展与实际 Worker 分配、有界退休/原分配结果重试、2,048 文件递归属性、完整目录移动/交换、硬链接递归删除、原结果重试、捕获的租约时钟、同摘要新增子项的 TTL 拒绝、替换 inode 保护、资格合并/取消/任期错误、有界位置清理及真实 Worker 删除确认。完整 hsm-rs/进程重启组合回归另记于性能优化方案；这些不是新的百万验收或原版性能对比。

复现 Curvine 回归：

```bash
CARGO_TARGET_DIR=/work/cargo-target CXXFLAGS=-g0 cargo test \
  -p curvine-master -p curvine-server --features hsm-scale-test \
  --lib --test hsm_optimization_test -j 6
```

### 属性结果的确定性编码

紧凑命令由各副本生成结果，不能依赖 HashMap 的进程内迭代顺序。SetAttrResponse 的 xattr 现在按键排序编码，其他 FileStatus 字段与现有 protobuf wire tag/type 保持；公共 DTO 仍是既有类型，已缓存的旧结果按原字节返回。新增回归先稳定复现原实现相同逻辑属性得到不同响应字节，再验证反复重新加载映射得到相同响应。当前 Master 260 项及两个真实集群用例通过；不改变历史 journal DTO 或 checkpoint 格式。

固定 Git `6a60ef60` 的完整 hsm-rs 回归已通过 30 项功能测试及文档测试，含五个 managed 场景与一个 native 场景。重启后原始属性响应逐字节比较也已通过，详见[最终组合验证](optimization-plan.md#最终固定来源组合回归代码-6a60ef60)。
