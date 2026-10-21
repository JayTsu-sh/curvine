# 普通元数据过渡微批：P1/P2 性能与闭环验收

日期：2026-10-21。before 为开启 HSM 的 P1 `826a2ae9be052b2bf86d1a1647551e396adfcc5f`；after 为开启 HSM 的过渡微批代码 `ad7fdaa2667cf1ce1c7fd614989c56e82d6560ec`。两组的普通修改均等待 Raft 多数派提交及本节点 apply。这里测的是 P1→P2 的增量；原版 Curvine→HSM 的总成本和 P1 的单独收益见[原始性能对比](performance-comparison.md)与[提交回执结果](commit-receipt-results.md)。

## 方法与构建身份

两组 release 二进制使用**完全相同**的 `curvine-server/examples/hsm_overhead.rs`（SHA-256 `3f05cc9ba38868d81e3de96a42aa36d5f5101c927e6c06f493519cc483ae07e7`），仅生产源码不同。before 二进制 SHA-256 `d20f95e2d923128a953602856e99cd6d4c64d8e7ad0bf186a38a703bdea48d49`；after 为 `ea96f4fa2ffd6804d8caa3d8980d6e6453cff84738f466f3f12451d62cdc6881`。驱动 `tools/hsm-overhead-compare.py` SHA-256 `8cddc2d96561a51f486b26c47644c24c674ad655b6084a11ec464113a25a67d5`。P1 比较工作树只复制了同一 benchmark example，未移植 P2 生产代码。

每次运行新建三个 Master、一个 Worker、Raft/RocksDB/journal 目录及客户端进程内 MiniCluster；经真实 loopback TCP/Raft，单数据副本、无 UFS、禁用短路。文件一直 Online，不运行控制器、data-mover、策略或归档。每项没有业务错误。before/after 顺序按轮交替；三轮分别取吞吐、P99 和提交计数的**中位数**，不合并原始延迟样本。所有运行串行完成，未并行编译或百万测试。开发 VM 与同进程故障域限制见[原始环境说明](commit-receipt-results.md)。

## 并发普通元数据：16 条 Master 连接

客户端显式设置 `master_conn_pool_size=16`；下表均为 16 个应用并发任务。`fence` 是该操作区间发出的提交标记数，`flush` 是实际 Raft proposal 数；两者口径不同，恰好在下列几项中相等。

| 操作 | 吞吐 before→after | P99 before→after | fence before→after | flush before→after |
|---|---:|---:|---:|---:|
| 256 次 create+complete | 202.2→1322.8 ops/s（6.54 倍） | 93.89→13.26 ms | 512→64 | 512→64 |
| 256 次 rename | 369.3→2521.2 ops/s（6.83 倍） | 63.73→7.57 ms | 256→32 | 256→32 |
| 256 次 delete | 395.0→2556.3 ops/s（6.47 倍） | 48.23→6.64 ms | 256→32 | 256→32 |
| 4 KiB 写入 | 122.0→753.5 ops/s（6.17 倍） | 158.27→33.82 ms | 384→53 | 384→53 |

多个普通修改共享 gate 持有期与提交前缀，真实 proposal 随之减少。组最多 32 个请求，但实际 flush 还受发送线程的条数与时间阈值影响；不能把 `fence` 当成普遍的磁盘提交次数。整个套件采样峰值 RSS 中位数为 164.0→158.7 MiB，运行时长中位数为 72.1→58.1 秒，均只代表这组 VM 工作负载。

读侧没有合批：16 并发 `read_4k` 为 92.84→96.19 ops/s、P99 187.57→192.72 ms，flush 均 1537、HSM 读屏障均 768；`open_close_empty` 为 94.53→91.89 ops/s、P99 197.08→209.73 ms，flush 均 1024。`get_status` 为 68152→59349 ops/s、P99 1.47→1.68 ms，出现约 13% 的吞吐下降；该只读路径未改动，现有同机测量不能确定其原因，也不能把它算作微批收益。读保护合批与隔离复测仍需单独进行。

## 默认一条 Master 连接

Curvine SDK 的 `client.master_conn_pool_size` 默认值为 1，同一客户端连接把本基准的并发 RPC 串行化。三轮 smoke 配对中，16 应用任务的 create+complete 为 187.19→189.90 ops/s、P99 85.35→84.13 ms；两组均为 32 次 flush。rename 为 378.45→369.48 ops/s，delete 为 382.56→387.79 ops/s；微批在该条件下没有实质集批收益。`get_status` 仍为 13802→12150 ops/s，需要隔离调查。此前固定 750 微秒聚集等待曾令单连接 smoke 的 16 并发 create+complete 仅约 110 ops/s，因此最终实现改为让出一次调度机会，不施加固定时间等待。生产部署如希望单客户端获得合批收益，应按并发度配置 Master 连接池；不同客户端进程的连接也可自然重叠。

## 64 MiB 大文件专测

另用每并发级 64 个文件做三轮 64 MiB 专测，仍为 16 条 Master 连接；4 并发写入中位吞吐 1163.2→1298.6 MiB/s，P99 463.98→459.09 ms，读取 2586.2→2568.9 MiB/s，P99 140.95→141.04 ms。单并发写入为 498.2→472.1 MiB/s，读取 697.5→684.4 MiB/s。逐轮波动很大：4 并发写入 before 为 873.5、1163.2、1350.2 MiB/s，after 为 1372.5、1298.6、1176.1 MiB/s；after 第二轮读取仅 1374.6 MiB/s，其余两轮约 2569 MiB/s。因此这些数字不能证明大文件路径稳定改善或回退。完整逐轮数据和 P99 见原始 JSON。

## 功能、HA 与规模边界

最终 `ad7fdaa2` 上 Curvine journal 单元 **27 通过**，Master HSM 相关单元 **8 通过、1 ignored**；协调器单测覆盖多请求一次 proposal、同任期、取消前后两个窗口以及 ACK 前 gate 不释放。hsm-rs 将八个 Curvine 依赖固定到该 Git 提交后，全工作区 `--locked` native-integration **30 通过、0 失败、2 ignored**，包含五项真实多进程集群测试；两个 ignored 是子进程 fixture 与需显式运行的规模测试。显式百万 namespace 测试在 `76c624f5` 上另 **1 通过**；随后 `ad7fdaa2` 仅将固定聚集等待改为一次调度让出并增加基准连接池参数，未改该规模测试或 Release/Recall 逻辑。规模测试内部批量预置 1,000,000 文件；零匹配策略扫描 1,000,002 条、3908 页；两个真实普通写入文件按策略 Release，三次普通读取自动合并为两次 Recall；排除旧 leader 后从 checkpoint 恢复，并核对文件身份、内容、驻留和每批最多 1024 键的清理上界。内部预置不等于百万普通 RPC 吞吐测试；三个 Master、一个 Worker 在同一 fixture 进程的独立目录，不等于跨物理故障域的 HA。固定 Git 依赖的测试详情见 hsm-rs 的 `docs/VALIDATION.md`。

本实现仍先在 leader 的内存投影执行普通 handler，再对本组日志提交；组内前一个请求的 speculative 修改可被后一个看到，但外部成功响应等到提交之后。普通/HSM 混合命令、持久普通请求结果去重、统一 committed apply、读屏障合批、LocalArchive 冗余和 Recall 私有布局多副本均未实现。遇到提交不确定时 Master 故障退出，由 Raft/checkpoint 恢复；这不是完整 P2 状态机或端到端生产 HA 验收。

## 原始数据与复现

三套结果各含六次完整运行：[16 连接元数据结果](mutation-microbatch-results/pool16-results.json)、[环境与二进制指纹](mutation-microbatch-results/pool16-environment.json)；[单连接 smoke 结果](mutation-microbatch-results/default1-smoke-results.json)、[环境](mutation-microbatch-results/default1-smoke-environment.json)；[16 连接大文件结果](mutation-microbatch-results/pool16-io64-results.json)、[环境](mutation-microbatch-results/pool16-io64-environment.json)。[百万 namespace 原始结果](mutation-microbatch-results/million-namespace.json)另存。

用同一 benchmark example 分别构建 P1/P2 release 二进制，再运行 `tools/hsm-overhead-compare.py --hsm-pair --rounds 3 --master-connections 16`，并指定两个二进制、对应 Git rev 与新的 `--output` 目录；默认连接数 smoke 加 `--smoke --master-connections 1`，大文件专测加 `--io-only --large-files 64`。驱动保存逐项 JSON、环境、二进制 SHA-256 和运行日志。
