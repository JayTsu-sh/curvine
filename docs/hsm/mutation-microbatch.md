# HSM 普通元数据过渡微批

状态：P2 的普通请求微批过渡步骤。统一 committed 状态机、普通请求的跨进程持久去重、HSM 条件命令与普通命令混合批、读保护合批尚未实现。P1 的自身日志回执及同任期本节点提交仍为基础，见 [提交回执](commit-receipts.md)。

## 执行与确认

每个 Master 的 FsDir 持有一个共享的 `HsmMutationBatcher`。普通 HSM 受管的元数据 RPC 在执行同步业务代码前入队；协调器短暂集批、取得现有 `hsm_mutation_gate`，然后依 FIFO 顺序逐个授予执行许可。每个请求仍在原来的连接处理器里运行原有业务代码，结果暂不返回。协调器收齐本组完成信号后只投递一个内存提交标记，等待该队列前缀由 Raft 多数派提交并在本节点 apply，再分别唤醒调用方。HSM 控制请求仍经同一 gate，因此不会观察到这组尚未提交的普通投影。

```text
普通 RPC 入队 → 获得本组 originating term → 原 handler 依次计算/写原生日志
           → 本组一个 commit fence → Raft commit + local apply
           → 每个 RPC 返回各自原结果 → 释放 gate
```

组大小最多 32 个请求，按 RPC header+data 长度估算的组大小最多 256 KiB，首个超限请求独自执行。协调器在取 gate 前让出一次调度机会，使已经就绪的并发请求入队；不设固定等待时间。待处理及执行中的请求以 128 个 semaphore 许可限流，满时调用方在**修改之前**等待。实际持久日志可能比 RPC 字节数大；该 256 KiB 是入组预算，不是已序列化 journal 的硬限制。发送线程原有条数/时间 flush 仍会切分 proposal，因此一次组提交标记不保证只有一次实际 proposal。只有实测的 `journal_flush_count` 能反映提交次数。

批处理的前提是多个 RPC 能同时到达 Master。Curvine SDK 默认 `client.master_conn_pool_size = 1`；单客户端此时在同一连接上串行发送，16 个应用任务也可能形成 16 个单请求批。HSM 多并发部署应按客户端配置增加 Master 连接池（本轮基准采用 16），并同时检查实际 `commit_fences` 与 `flushes`。不同客户端进程的连接也可能自然重叠；连接数增加本身不保证每批都满 32 个请求。

每组在取得 gate 后固定 term。执行许可发送给客户端协程后，还需收到它真正开始执行的握手：断连发生在握手前可直接跳过，握手后到同步 handler 结束之间没有 await。执行完成后即使客户端取消等待，协调器仍持有 gate、完成提交；若已开始的 handler 意外消失，或提交结果不确定，则 Master 故障退出，由 Raft 日志与 checkpoint 恢复，不能释放锁继续服务 speculative 投影。HSM 控制操作也在独立任务中持锁至完成；不确定的控制结果在放锁前用读屏障确认队列前缀，屏障无法确认时故障退出。

这仍是过渡实现：普通 handler 先修改 leader 内存投影，随后提交日志。不同普通请求可在同一 gate 持有期内看到前序请求的 speculative 修改；它们直到整组 commit 后才对外成功。不能把 gate 提前释放，更不能让 HSM 条件命令插入组内。完整 P2 的统一 committed apply、持久请求身份/结果和混合命令排序见 [性能优化方案](optimization-plan.md)与 [HA 设计](ha-design.md)。本组内任一普通 handler 返回业务错误，仍由其原协议单独返回；协调器必须提交已产生的全部日志前缀，不能因该错误跳过其他请求的确认。

## 验收边界

2026-10-21 的单元测试检查多请求共用一次 proposal、固定 originating term、取消前/后两种窗口及提交前 gate 不释放；最终提交 Curvine journal **27 项通过**，Master HSM 相关 **8 项通过、1 项独立规模基准 ignored**。hsm-rs 联合工作区此前本地源码配对 **30 项通过、2 项 ignored**，其中五项是真实多进程受管集群测试，覆盖策略 Release、普通读取自动 Recall、进程重启和换主。最终固定 Git 依赖复验见[性能与闭环验收](mutation-microbatch-results.md)。

显式百万 namespace 测试在最初 P2 提交 `76c624f5` 上另 **1 项通过**。内部批量预置 1,000,000 个普通文件，通过公开 SDK 核对数量；零匹配策略扫描 1,000,002 项、3908 页；两个普通写入的非空文件按策略 Release，三次普通读取触发两次自动 Recall。排除旧 leader 后从 checkpoint 恢复，逐项核对身份、驻留、内容及有界清理批次。原始结果见 [million-namespace.json](mutation-microbatch-results/million-namespace.json)。其后 `ad7fdaa2` 只改变普通请求聚集前的调度，不改该测试或 Release/Recall 路径。这不是百万普通 RPC 写入吞吐测试，三个 Master 和一个 Worker 仍在同一 fixture 进程的独立目录中，不构成跨物理故障域 HA 验收。

三轮同语义 release 基准已将 P1 二进制与本轮二进制交替测量，分别报告吞吐、P99、每组 commit fence 与实际 flush；另测默认单连接和加长大文件工作负载。结果与原始 JSON 见 [过渡微批性能对照](mutation-microbatch-results.md)。
