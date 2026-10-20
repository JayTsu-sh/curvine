# 原生生命周期策略与持久任务

实现 v0.5，2026-10-20。运行命令和完整本地文件场景见 [hsm-rs LOCAL-HSM](https://github.com/JayTsu-sh/hsm-rs/blob/feat/curvine-adapter/docs/LOCAL-HSM.md)。

## 职责

Curvine 原 common CF 保存策略、任务、每实体 active index 和源原语结果。外部 hsm-rs worker 负责定时调用、后端选择、复制与结果对账；没有外部文件清单或元数据镜像。扫描的触发定时器在外部，候选条件计算和持久入队在 Master 内部。

策略选择新建不可变 `/.hsm/<name>` 的文件名前缀、最小/最大大小、当前驻留状态的持续时间；动作是 Archive、Release 或显式 Recall，目标只含逻辑 backend ID。Archive 选择 Online；Release 要求 Dual、匹配 anchor、无 view/private layout；Recall 要求 Offline 且有对应 anchor。正数时间门槛不接纳缺少起始时间的旧记录；当前不是 atime 或容量水位策略。

## 有界原子入队

policy_put 条件检查原 revision，新策略从 1 开始，更新为 N+1；关闭或修改策略会使未执行旧任务在 claim 时失效。

policy_scan 每次最多读取 16 条原生 HSM record，cursor 严格位于最后检查的 native key 之后，不只覆盖匹配项。使用原 RocksDB 有界范围 iterator。分页不是 namespace 快照，新一轮从头扫描补齐新增文件。

一个提交事务同时校验策略原值、所有候选 record version、确定性 intent key 不存在，以及实体 active index 不存在；然后同 batch 写入任务与 active index。并发变更使整个批次失败，不能推进 cursor 后遗漏候选。记录属于同一原生复制权威域，不新增数据库。

intent ID = hash(policy ID, policy revision, DataKey)。同一规则版本/内容 generation 最多一次，实体同时最多一个未完成策略任务；重扫不重复，多个扫描器争用由 CAS 决定。规则改版会重新开放选择，因此操作员需避免互相冲突的长期冷热规则。

## 执行资格与完成

Ready → Running，由新 claim ID 和任务 record version 保护。当前 controller epoch 内的 Running 不抢占；接管先条件提升全局 controller epoch，再领取旧 epoch 工作。旧控制者的新源修改/完成被 Fenced，没有超时即删除或隐式假定旧动作失败。

effect operation ID 始终来自 intent ID。控制者 epoch 只决定修改资格，不改变逻辑请求指纹；pending owner 仍绑定 operation ID/attempt。新资格可以查询、恢复同一 view/private allocation，最终 apply 再检查当前 epoch。Worker 的独立封存与墓碑机制继续撤销旧写 capability。

finish Done 必须有匹配 DataKey/action 的源端原始成功 outcome，执行器上报无法自行构造成功。原始成功优先于失败/重试报告，包含新 claim 时对已提交动作的对账。确认结果或终止任务与清除 active index 同 batch；不确定结果回 Ready 并保留原 effect ID。已失败任务与资源引用仍可查询，不按超时释放保护。

## 日志兼容与恢复

policy/intent 控制 CAS 使用原 Hsm journal 命令，但 payload 带独立 `hsm-lifecycle-v1` 前缀。旧实现不能把未知 controls 当作无操作而持久化成功：它会解析失败并按 HSM apply 规则停止。全部 Master 必须先升级到本实现，再启用策略。

旧 HSM JSON 事务仍可读取；新 controls/record_checks/state_since_ms 使用默认值。策略和任务前缀同样由启动时的 HSM projection 重建及既有 checkpoint 覆盖，无外部重建镜像。

## 验证与限制

真实三 Master/单 Worker 测试通过 CLI 独立进程导入与执行策略，覆盖开关与 revision CAS、大小/前缀/时间门槛、逐条分页、去重和 active index、重复领取拒绝、虚假 Done 拒绝、换主重启后的策略/任务/epoch、源成功但任务未完成的对账、封存至后端 receipt 发布窗口恢复，以及损坏归档不得释放。恢复后核对完整字节，未匹配文件保持 Online。

当前仍为不可变直接子文件、本地归档、显式 Recall、attempt epoch 1、按 key 线性筛选。尚无原地转管、可变文件、透明召回、访问热度/容量水位策略、普通块统计/副本修复、完整生产故障矩阵或带库。
