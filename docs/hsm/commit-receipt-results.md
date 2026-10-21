# 普通日志提交回执：改造前后性能验收

日期：2026-10-21。这里比较的是 **HSM 开启前后的两个 HSM 实现**：旧版在普通修改提交后追加一个空 HSM barrier；新版等待本次普通日志的 commit/apply 回执，并校验 originating term。两组都自动纳管普通文件，使用相同的应用工作负载、journal WAL 同步和返回确认要求。原版 Curvine 与 HSM 的差异另见 [基线报告](performance-comparison.md)，不能与这组百分比混算。

## 测量边界

旧生产源码 `837b5cc4a508fc194de9a3e08dc32f00bc76674c`，release 二进制 SHA-256 `fe20a445c5a675c01f6ac3554af546a2d1eabee21360e6c4c998898df467dd39`；新生产源码 `826a2ae9be052b2bf86d1a1647551e396adfcc5f`，release 二进制 SHA-256 `3b17b0ef23435c8fa4d87e7190b4d4b7d08f5e0da22adff384b47dc25b7789a7`。新旧源码只改变生产提交路径、相应的测试和基准计数；原工作负载与参数相同。两组开启 HSM，文件保持 Online，不启动 hsm-rs controller、data-mover 或归档后端。

每组在同一开发 VM 上运行三轮，交替执行 before/after、after/before、before/after；每次新建进程、三个 Master 和一个 Worker 的独立目录。RPC/Raft 为真实 loopback TCP，单数据副本，无 UFS，禁用短路；日志 WAL 同步写。全部 release 二进制在测量前构建，正式测量不并行编译和百万规模测试。元数据套件和大文件专测分别运行；大文件专测每并发级别 64 个 64 MiB 文件。表内吞吐和 P99 采用各轮值的中位数，吞吐变化以两个中位数计算；不能当成生产物理多机吞吐或跨物理故障域 HA。

## 安全和规模复验

Master journal 26 项、Raft 实节点 2 项、Master HSM/恢复 8 项，以及 hsm-rs 固定 Git 依赖的 native-integration 全工作区 30 项均通过。显式百万 namespace 测试 1 项通过，原始数据见 [million-namespace.json](commit-receipt-results/million-namespace.json)。它检查 1,000,002 个候选、3908 页，两个文件 Release、三次普通读取合并为两次自动 Recall，并在排除旧 leader 后验证 checkpoint 恢复。百万目录通过内部批量预置，不能推断百万普通 RPC 写入吞吐；该轮曾与构建重叠，耗时不拿来和旧版比较。

## 前台性能与提交计数

元数据与 4 KiB 操作的三轮中位数如下。每组创建 256 个空文件（包括 complete），其余元数据操作各 256 次；4 KiB 写各 128 个文件，读 384 次。表内 P99 单位为毫秒。

| 操作 | 并发 | 旧版 ops/s | 新版 ops/s | 吞吐变化 | 旧版 / 新版 P99 |
|---|---:|---:|---:|---:|---:|
| 创建并关闭空文件 | 1 | 96.7 | 180.8 | +86.9% | 13.83 / 7.22 |
| 创建并关闭空文件 | 16 | 95.7 | 198.1 | +106.9% | 205.84 / 104.28 |
| rename | 1 | 176.4 | 352.7 | +99.9% | 9.19 / 3.42 |
| rename | 16 | 205.3 | 389.2 | +89.6% | 83.75 / 47.50 |
| delete | 1 | 181.6 | 359.1 | +97.8% | 8.60 / 3.14 |
| delete | 16 | 202.0 | 400.4 | +98.2% | 104.63 / 42.43 |
| 写入并关闭 4 KiB | 1 | 62.3 | 108.3 | +73.7% | 17.75 / 10.78 |
| 写入并关闭 4 KiB | 16 | 68.5 | 131.0 | +91.2% | 238.82 / 133.08 |
| 打开并关闭空文件 | 16 | 90.7 | 95.6 | +5.4% | 191.23 / 184.30 |
| 完整读取并关闭 4 KiB | 16 | 97.1 | 94.1 | -3.1% | 190.78 / 189.78 |
| get_status | 16 | 12,996.4 | 13,579.3 | +4.5% | 1.69 / 1.45 |
| 列出 256 个子项 | 16 | 1,533.6 | 1,680.8 | +9.6% | 11.25 / 9.73 |

新二进制按区间采集真实 journal 计数：空文件 create+complete 的 256 个客户端操作生成 512 个 commit fence、512 次 flush、**0 个空 HSM barrier**；256 次 rename 或 delete 各生成 256 个 fence、256 次 flush、0 个空 barrier；128 个 4 KiB 写入+complete 生成 384 个 fence、384 次 flush、0 个空 barrier。统计在三轮中相同。open+close 256 次则仍有 512 个空 HSM barrier、1024 次 flush、0 个普通日志 fence，说明读取保护路径尚未优化。旧二进制没有同一计数探针；“旧版额外 barrier”来自源码结构，不能把新计数冒充两版直接差分。

普通小文件修改吞吐约增 74–107%，对应 P99 下降。open/close 与只读操作变化较小，短窗口还受 VM 负载和缓存影响，不能把 get_status/list 的正变化归因于这次提交改造。当前实现仍保留全局 HSM gate，提交回执减少额外 proposal，但串行等待仍限制小文件吞吐；没有完成 P2 微批。

完整三轮元数据及短数据测试的各轮值、P50/P95/P99、进程资源和计数见 [metadata-runs.json](commit-receipt-results/metadata-runs.json)，[环境与二进制指纹](commit-receipt-results/metadata-environment.json)及[源码指纹](commit-receipt-results/source.json)。

## 64 MiB 在线读写专测

每并发级别写 64 个文件，共 4 GiB；读取 192 次，共 12 GiB。数据保持 Online，读取路径没有 Recall。每组仅执行大文件专测，在六个新建集群中交替运行三轮。

| 操作 | 并发 | 旧版 MiB/s | 新版 MiB/s | 变化 | 旧版 / 新版 P99 ms |
|---|---:|---:|---:|---:|---:|
| 写入并关闭 64 MiB | 1 | 426.4 | 447.0 | +4.8% | 422.19 / 376.15 |
| 写入并关闭 64 MiB | 4 | 1,040.8 | 1,166.3 | +12.1% | 369.44 / 489.81 |
| 完整读取并关闭 64 MiB | 1 | 619.2 | 658.2 | +6.3% | 192.13 / 153.09 |
| 完整读取并关闭 64 MiB | 4 | 2,497.7 | 2,399.1 | -3.9% | 143.70 / 153.38 |

新版 64 文件写入有 192 个普通 commit fence、0 个空 HSM barrier；读取有 0 个普通 fence、384 个读取保护 barrier。每轮值见 [long-io-runs.json](commit-receipt-results/long-io-runs.json)和[环境](commit-receipt-results/long-io-environment.json)。4 并发写入吞吐中位数提高 12.1%，但 P99 反而从 369.44 增到 489.81 ms；4 并发读取略降。各轮波动明显，不能以这组三轮样本宣称大文件吞吐或尾延迟得到稳定改善。大文件传输时间占比更高，P1 仅减少固定提交成本。

## 复现

构建并分别保存上述两版 `curvine-server/examples/hsm_overhead.rs` 的 release `hsm-scale-test` 二进制；同一基准源码、关闭短路、`BENCH_MODE=on`。从本仓库执行：

```sh
python3 tools/hsm-overhead-compare.py \
  --output /tmp/hsm-p1-paired \
  --original-binary /path/to/before/hsm_overhead \
  --hsm-binary /path/to/after/hsm_overhead \
  --original-rev 837b5cc4a508fc194de9a3e08dc32f00bc76674c \
  --hsm-rev 826a2ae9be052b2bf86d1a1647551e396adfcc5f \
  --rounds 3 --hsm-pair
```

大文件专测对新的空输出目录重复执行，并加 `--io-only --large-files 64`。原始结果和环境指纹随本报告保存。该驱动把 before/after 两组都设为 `BENCH_MODE=on`，避免把关闭 HSM 的行为误当成旧实现。
