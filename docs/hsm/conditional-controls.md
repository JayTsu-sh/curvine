# 文件级 HSM 控制的并行提交

本轮把成功的文件级控制操作从独占等待改为共享资格准备、Raft 排序、条件发布。Curvine 仍持有文件记录、工作流绑定和原始结果；外部 hsm-rs 仍协调 Archive、Release、Recall，没有新增元数据镜像。

## 分配身份，再准备效果

Native 创建和私有 HSM Import 共用 `FsDir.inode_id` 的原子预留器。不同私有准备视图不再从自己的 `last_inode + 1` 分配；准备失败可以产生空洞，但不会把同一个 inode 给另一项并发操作。预留本身不创建 inode、不写 RocksDB，也不允许 Worker 提前发布数据。

committed apply 仍按日志记录推进持久 inode floor。内存 floor 的推进使用原子 max 语义：如果并发准备已预留更高 ID，重放较低 floor 不回退、不报错。进程重启仅恢复已提交身份；未提交且未产生外部效果的预留无需持久保存。

## 成功操作如何并行

1. 首次 namespace 创建或格式升级仍在独占资格内完成。绑定到已有 namespace 的新 SDK 会话使用共享只读资格，不再排独占队列。
2. 文件控制取得所属 Recall/前台/后台类别的共享资格，执行 committed ReadIndex 预检。
3. 在短暂的 FsDir 读锁内读取 Record、准备新状态，记录 inode、版本、epoch、路径和稳定工作流身份。准备过程不修改权威投影。
4. 使用 `hsm-conditional-v1` 日志封装提交。不同文件可同时等待 Raft；一个文件的并发操作由 apply 的版本检查决定先后。
5. apply 先查询原始结果；已完成身份返回原结果。新计划的版本、路径或 create 条件过期时返回 **Busy**，不写 Record、绑定、索引或 outcome；调用方用同一逻辑身份重新准备。
6. 条件成立时沿用原有 WriteBatch，把 Record、派生索引、退休事实、工作流绑定和 outcome 一起发布。Worker Allocate/Seal/Delete 继续在持久授权之后执行，等待网络时释放资格，返回后重新验证 namespace 和 epoch。

支持共享等待的文件动作包括 Prepare/CommitImport、Prepare/CommitArchive、Open/CloseView、Release、Prepare/Commit/AbortRecall，以及 SealBegin/SealCommit。namespace、inspect、resolve、稳定视图块查询、pending/completed 查询、策略/任务列表和任务详情等只读控制也共享资格；它们不创建工作流结果。

## 过期计划与业务失败

并发 CAS 冲突说明准备依据已经过期，不能把它固化为同一 operation ID 永久失败。因此新封装在任何投影/WriteBatch 修改之前拒绝过期计划。Busy 是一次确定的未发布结果，不是一个成功工作流的持久回执。

业务检查失败时，操作释放共享资格、重新申请独占资格、重新通过 ReadIndex/namespace/epoch 检查，并**从头执行整个请求**。只有独占路径重新确认的业务拒绝才按已有规则保存结果；不能升级资格后直接保存先前的错误。网络不确定结果仍执行既有前缀确认/隔离，不能当成 Busy 丢弃。

GC 使用相同条件封装确认最新 Record 中的退休事实。Worker 删除成功而确认 CAS 过期时保留 outbox，后续重试核对，不回滚已提交的 Release。

## HA 与兼容性

新封装的语义只作用于新日志。历史普通 Transaction 和 lifecycle/read-batch 的解码及持久冲突结果不变。新增空 CF `hsm_native_v7` 阻止旧二进制重新打开含新语义的 checkpoint；业务数据继续存于既有 CF，没有复制完整 namespace。

所有 Master 必须使用匹配版本。此处没有增加滚动激活协议，也没有把本地公平队列变为持久权威。取消 RPC 调用者后，已受理操作仍由 owned task 完成提交或隔离未知前缀。

## 本轮仍保留的独占边界

controller epoch 修改、策略修改/筛查入队及任务 claim/finish、显式批量 GC 和旧式 RPC/后台副作用继续独占。策略容量预算等跨记录条件需要单独的提交检查，不能只因为 Record 已支持 CAS 就一并移除隔离。普通读取合批和 owned Native 路径沿用此前的共享实现。

本页描述实现与约束；回归结果、提交号及尚未执行的规模验收见[优化方案](optimization-plan.md)。没有以本轮改动推算最终性能收益。
