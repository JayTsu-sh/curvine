# Master 连接内并发与读取合批

本文记录第一阶段仅调整 RPC 的实现及当时的测量。后续已批准的访问观察批量持久化和快速授权见[受保护快速读取](fast-read-protection.md)，其结果不与本表混算。

2026-10-23。修复 Master 在单个 TCP 连接上等待当前 RPC 完整完成后才读取下一条的问题。客户端 `BufferClient` 已支持多个在途请求和按 request ID 匹配响应，RPC 服务端现在使用 `HandlerService::is_stream()` 选择处理方式。

- 流式服务继续使用 `StreamHandler`，按连接顺序执行请求。
- Master 开启 `master.meta_request_concurrent` 时使用 `ConcurrentHandler`：独立读取完整请求，取得每连接和共享全局许可后派发，所有响应通过一个有界队列交给唯一的写帧任务。
- 响应可按完成顺序返回；一个响应帧的协议头、业务头和 body 完整发送后才发送下一个帧，避免交叉写入。
- WorkerHeartbeat、WorkerBlockReport 和 ReportBlockReplicationResult 共用跨连接共享的独立 8 个有界名额，防止普通客户端或慢接收者占满全局名额后阻塞存活心跳。Worker 心跳循环会同步等待增量块报告，复制结果也复用同一 MasterClient；这些控制请求都需要保留通道，避免在同连接上堵住后续心跳。每连接上限继续适用；保留名额不能绕过同连接中已阻塞的普通请求，也不保证保留通道自身被占满时仍有额外容量。
- 许可持有到响应发送完成，约束执行中的请求和等待客户端接收的响应。默认每连接上限为 `master.conn_limit=8`，每 Master 全局上限为 `master.global_limit=4096`。
- 收包时间在取得许可前记录，使服务端排队时间包含入场等待。活跃请求不会被连接空闲超时终止；部分解码的请求在继续等待时保留其解码 future。
- EOF 停止接收并排空已接受请求的回复。写入失败停止接收新请求；已接受的 handler 继续执行，无法送达的结果由现有请求身份与 outcome 机制对账。关闭连接不等于取消一个可能已提交的修改。
- 业务 future 由同一连接直接并发推进，避免为每条小 RPC 增加一个调度任务；断连时把剩余 future 移交后台排空。同步 handler 继续使用既有阻塞线程池。
- 缓冲 RPC 使用 `write_vectored` 一并发送协议头、业务头和 body，减少小帧的写入调用；短写后按实际发送字节推进各片段，保持原有线格式。

业务分派及同步/异步错误封装收敛到同一内部 `dispatch`，由两种处理方式共用。Master 的 reader 授权仍在 Raft 提交与本地 apply 后返回；读取关闭仍提交原有 `read_close`。此次修复通过提高入批并发减少共识轮次的重复支付。

代码：[连接分派](../../crates/core/rpc/src/server/rpc_server.rs)、[并发处理](../../crates/core/rpc/src/handler/concurrent_handler.rs)、[公共业务分派](../../crates/core/rpc/src/handler/message_handler.rs)。`master.meta_request_concurrent=false` 可切换回串行处理。

## 验证

```bash
cargo test --locked --offline --release -p curvine-rpc --tests
cargo test --locked --offline --release -p curvine-server --features hsm-scale-test --test hsm_optimization_test
cargo clippy --locked --offline --release -p curvine-rpc --tests --no-deps -- -D warnings
```

RPC 的 24 项测试通过，其中新增 9 项真实 TCP 测试覆盖乱序回复、同步/异步错误、流式顺序、每连接及全局限流、慢客户端、断连后完成、半关闭排空、活跃请求的空闲超时和 panic 清理；另有一个限制发送缓冲区的大帧测试验证短写后的协议头、业务头及 body 完整性。相同的单连接并发用例在基线 `ed27bcda` 上超时，能够捕获原问题。

HSM 的两项集群回归通过。原有三 Master/一 Worker 用例改为 `master_conn_pool_size=1`，验证单连接读取合批、活跃 reader 阻止覆盖、提交关闭后解除保护，以及原生写入与透明读取；另一项验证关闭 HSM 时的普通操作确认与 namespace fencing。

## 性能记录

同一 Release 分段基准、HSM 开启、Master 连接池 1、默认连接内上限 8；每组为新进程及新目录的三 Master/一 Worker MiniCluster。交错执行三轮，以下为各轮中位数。每档对 256 个已有空文件执行普通 SDK `open()` 与 `complete()`。

| 指标 | 修复前 | 修复后 |
|---|---:|---:|
| 16 并发吞吐 | 152.4 ops/s | 578.3 ops/s |
| 16 并发 P99 | 113.98 ms | 35.69 ms |
| 16 并发平均延迟 | 103.56 ms | 27.15 ms |
| 单并发吞吐 | 144.2 ops/s | 145.6 ops/s |
| 16 并发阶段的 authority proposal | 约 512 次 | 约 107–110 次 |

16 并发吞吐为原来的 3.79 倍，P99 降低约 68.7%。读取命令从近乎每批一条变为约每批 4.8 条。单并发仍支付授权与关闭的固定事务成本。

关闭 HSM 的普通路径另以每档 4096 文件交错执行三轮：单并发吞吐中位数为 6125.8 → 6033.8 ops/s（-1.5%），P99 为 0.220 → 0.252 ms；16 并发吞吐为 18744.8 → 28872.5 ops/s，P99 为 1.238 → 0.735 ms。普通路径未再出现早期并发实现约 16% 的单并发吞吐下降，仍有约 32 微秒的单并发 P99 差值。

这些结果来自共享虚拟机、同进程 TCP/Raft，并非生产多物理节点容量；每组仅三轮，不能据此声明统计显著性。阶段指标在 RESULT 后抓取，可能与相邻阶段重叠少量命令。
