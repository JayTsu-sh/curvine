# 生命周期控制的共享条件提交

策略更新、扫描入队和任务 claim/finish 已从独占等待改为共享准备、Raft 排序和条件发布。显式 GC 入口也共享资格，内部仍只根据已提交的退休事实删除或确认布局。Curvine 保留权威记录，hsm-rs 继续协调动作，未引入外部元数据镜像。

## 一次操作怎样完成

1. Source 在有界的 Recall/前台/后台接入名额内受理请求，取得共享资格并通过 committed ReadIndex 预检。
2. 策略扫描仍先固定 RocksDB snapshot，再离开资格区做最多 256 行、10 毫秒、2 MiB 的过滤；扫描并发上限仍为 4。入队阶段重新取得**共享**资格，复查当前状态，最多接纳 16 个文件。
3. Source 在短暂的 FsDir 读锁内准备 Transaction，同时收集本次决策真正依赖的检查集。它不克隆全目录，也不把检查集维护成另一份文件数据库。
4. 新封装 `hsm-lifecycle-conditional-v1` 把 Transaction 和检查集一同写入 journal，使用准备时的 originating term。准备完成后，不再用独占 gate 等待 Raft。
5. committed apply 先查询原始结果，再复核检查集；依据过期返回 Busy，namespace 资格改变返回 Fenced。此时不修改 Record、策略游标、任务、预算、binding 或 outcome。
6. 检查成立才沿用原有 WriteBatch，原子发布策略/任务、工作索引、活动标记、预算和原始结果。取消 RPC 调用者不取消已受理提交；提交结果未知的既有前缀确认与隔离继续保留。

apply 仍串行持有 FsDir 写锁。共享等待减少的是提交前后的相互阻塞，不表示 RocksDB 状态机可以无锁修改。

## 检查集包含什么

| 决策依据 | 提交时如何检查 | 避免的问题 |
|---|---|---|
| namespace | 保存并比较 namespace 原始值 | 旧 storage/incarnation/epoch 资格继续生效 |
| policy、PolicyRun、intent、active | Transaction 原有 expected 值逐键 CAS | 策略被修订、游标并发推进、同文件重复入队、任务被重复认领 |
| 入队候选 | Record 合并版本、当前路径、文件副本数 | 新访问/内容变化后仍入队，改名越过策略范围，副本变化造成预算计算不一致 |
| claim 依据 | 当前 Record 的存在性、版本、路径和副本数 | 用旧文件观察或旧缓存路径认领任务 |
| 动作完成结果 | 读取 outcome 时记录其原始值，**包括不存在** | 已成功动作被一个更早准备的失败/重试报告覆盖 |
| 容量策略待释放预算 | 比较 `budget:online:v1` 的原始值 | 两个扫描基于同一待释放预算同时继续预留 |

检查集只覆盖候选和相关键，不使用“整个卷任意修改就失效”的全局版本。不同文件、不同策略的无容量约束提交可以同时准备并依次成功。同一个容量预算被其他入队或 finish 改变时，需要重新计算；与它无关的普通文件读取不会使该预算检查失败。

claim 的筛选路径从当前 inode namespace 解析，不再直接使用 Record 中可能缓存的旧路径。副本数参与物理字节估算，单独检查它，不能假设所有 inode 属性变化都一定推进 Record.version。

## 水位与预算分别保证什么

高低水位仍根据 Worker 上报的可分配容量/可用量决定。该观测样本随新封装保留，重放不会读取 follower 自己的 WorkerManager 来重新决策。

`budget:online:v1` 是已复制的逻辑待释放预算，由任务入队/完成在同一个 WriteBatch 中维护。并发扫描必须通过预算旧值检查，不能各自使用同一个旧余额发布。若预留总量变化，重试会使用新的总量重新筛选。

Worker 容量本身仍是异步遥测。这里没有把它改造成线性一致的物理空间预留，最后一个文件可能按整文件粒度跨过目标水位；这与旧策略的采样和整文件语义一致。一次扫描最多接纳 16 项，后续扫描重新取样、复核低水位。数据安全仍由动作执行时的 generation、稳定视图、锚点和读取租约检查保证。

## 完成报告为什么也需要检查

例如执行器准备 `Failed` 报告时尚未查到 CommitArchive 结果，但 Source 在报告提交前已经完成归档。新封装会发现 outcome 从不存在变成已提交，拒绝旧报告且不保存其失败结果。用原请求重试后，Source 优先认领已提交动作，任务进入 Done，释放活动标记和对应预算。

相反，如果任务或 claim 自身已经变更，原有 intent CAS 会拒绝旧报告。资格 epoch、claim token 与版本检查继续存在，不能凭执行器自己的 Done 报告确认数据效果。

## Recall 不能在认领阶段重新挤进后台队列

`intent_claim`、`intent_finish`、`intent_get` 根据存储中任务的不可变 action 选择 Recall 或后台类别；同一个类别贯穿接入名额和 AuthorityGate。后台 128 个接入名额耗尽时，Recall 任务仍使用自己的名额。客户端提供的类别不会覆盖真实任务 action。

这个无 ReadIndex 的本地读取只作调度提示，不能授权状态变化。缺失或无法解码时采用普通分类，真正操作仍执行 ReadIndex、namespace、claim 和提交条件检查。读取列表的 work_class 索引分页及 4:8:1 资格权重保持原行为。

## 资源与版本边界

每个新命令最多 34 个控制键、16 个文件观察、3 个公共键观察，总编码不超过 1 MiB。编码和解码均检查边界，重复或越界的检查集拒绝应用。新的检查集放在独立封装里，不修改旧 Transaction/read-batch DTO，不改写历史冲突结果。

新增空兼容性 CF `hsm_native_v8`，业务记录仍在原 CF 中。旧二进制不能打开含新语义的 checkpoint，全部 Master 必须使用匹配代码；没有新增混合版本滚动激活协议。

namespace 创建/升级、controller epoch 修改、文件动作业务拒绝的独占重新确认、兼容 RPC 和旧后台副作用仍保留独占边界。共享 GC 不代表所有旧式 Worker 删除已迁移持久 outbox。当前剩余项及验证结果见[优化方案](optimization-plan.md)。
