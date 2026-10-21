# 引入 HSM 相对原版 Curvine 的性能差异

日期：2026-10-21。本次使用 release 构建和普通 SDK RPC，实际比较引入 HSM 前后的前台性能。上一份 [百万规模报告](scale-validation.md) 中的“旧算法”仍有 HSM，不能用于回答原版 Curvine 与 HSM 的差异。

**实测结论：当前 HSM 实现的每文件操作开销很大。** 16 并发创建/关闭空文件吞吐中位数从 5690.5 降至 96.9 ops/s，打开/关闭从 13292.9 降至 94.0 ops/s；get_status 基本持平。在线大文件读写也受固定协调成本影响，具体幅度见加长测试表。优先优化提交串行化与额外 barrier，比继续减少同键索引写入更直接。

## 版本和测量边界

| 组 | 生产代码基线 | 运行状态 |
|---|---|---|
| original | `feb3c9e20617d5c3f36c67d44033239b6ac92470` | 引入 HSM 前的原版，原版 SDK |
| off | `837b5cc4a508fc194de9a3e08dc32f00bc76674c` | 新版 SDK/服务器，hsm_enabled=false |
| on | 同上 | hsm_enabled=true，提前初始化 namespace，普通写入自动纳管，保持 Online，无数据搬运 |

三个运行组在各批次使用逐字相同的基准源码。首批完整套件的基准提交为 `11e8f3e1`，后续只增加大文件计数和 io-only 开关；[当前基准源码](../../curvine-server/examples/hsm_overhead.rs) 支持两种运行方式。原版只复制基准文件，不修改业务代码；新版只增加基准文件，没有进一步优化生产代码。HSM 构建开启既有测试 feature `hsm-scale-test` 以编译控制辅助代码，**不调用内部批量预置函数**。

每组运行三轮，每次使用全新进程、RocksDB、journal 和 Worker 目录；执行顺序为 original/off/on、off/on/original、on/original/off。两组构建都在测量前完成，无 Cargo 构建与正式测量并行。表内为三轮吞吐中位数，P99 为每轮 P99 的中位数，不是合并样本分位数。原始数据保留各轮值及范围。元数据/4 KiB/扫描完整套件共 9 次集群运行；64 MiB 的短测波动较大，因此另做每套 64 文件的大文件专测 9 次集群运行，正文大文件表只引用加长后的结果。两批之间没有修改生产代码。

环境为同一开发 VM：24 个可用 vCPU、约 31 GiB 内存，Linux x86_64，`rustc 1.98.1`、release opt-level=3、CXXFLAGS=-g0。数据与日志位于 `/work` 的 XFS 本地虚拟 SSD；独立 4 KiB write+fsync 探测中位约 0.651 ms。主机还有其他服务，未全局清缓存。

每次是一个 MiniCluster 进程内的三个 Master、一 Worker 和 SDK，使用真实 TCP/Raft 和独立元数据目录；单数据副本、无 UFS、禁用短路访问。journal 开启、writer_channel_size=0，保留默认 10 ms/1000 项普通日志批量参数；Raft WAL 同步写保持开启。关闭本轮 checkpoint 触发以隔离前台路径。该结果不是物理多机网络吞吐或 PVE 集群的 A/B。

**返回确认语义存在差异。** 原版普通修改将 journal 入队即可返回；开启 HSM 后普通修改在共享串行边界内等待额外提交屏障，受保护的 read-open/read-close 也提交权威记录。两组 Raft 存储都启用 WAL 同步刷盘；差别在前台是否等待及如何分批。因此下列值是实际产品行为的总差值，不能全部归因于 Record/索引的 JSON 或 RocksDB 写入。

## 元数据操作

每套 256 个空文件；create 包含 complete，open 包含 reader.complete。get_status 执行 1024 次；目录列举执行 64 次，每次返回 256 项；rename/delete 各 256 次。父目录准备和最终清理不计入表内计时，先完成独立小规模预热。

| 操作 | 并发 | 原版 | 新版 HSM 关闭 | 新版 HSM 开启 | 开启相对原版 |
|---|---:|---:|---:|---:|---:|
| 创建并关闭空文件 | 1 | 2,595.1 ops/s | 2,589.0 ops/s | 89.6 ops/s | -96.5% |
| get_status | 1 | 5,809.0 ops/s | 6,165.3 ops/s | 5,598.3 ops/s | -3.6% |
| 打开并关闭空文件 | 1 | 5,778.0 ops/s | 3,337.8 ops/s | 92.0 ops/s | -98.4% |
| 列出 256 个子项 | 1 | 1,257.4 ops/s | 1,311.8 ops/s | 1,228.1 ops/s | -2.3% |
| rename | 1 | 5,063.7 ops/s | 5,562.0 ops/s | 184.2 ops/s | -96.4% |
| delete | 1 | 5,219.2 ops/s | 5,412.4 ops/s | 174.2 ops/s | -96.7% |
| 创建并关闭空文件 | 16 | 5,690.5 ops/s | 5,753.1 ops/s | 96.9 ops/s | -98.3% |
| get_status | 16 | 13,042.2 ops/s | 12,703.9 ops/s | 13,148.9 ops/s | +0.8% |
| 打开并关闭空文件 | 16 | 13,292.9 ops/s | 8,567.7 ops/s | 94.0 ops/s | -99.3% |
| 列出 256 个子项 | 16 | 1,681.2 ops/s | 1,701.3 ops/s | 1,720.1 ops/s | +2.3% |
| rename | 16 | 10,886.4 ops/s | 10,163.3 ops/s | 203.4 ops/s | -98.1% |
| delete | 16 | 10,967.1 ops/s | 11,054.8 ops/s | 194.7 ops/s | -98.2% |


create/open/rename/delete 的并行吞吐受 HSM 全局提交串行边界约束，增加前台并发不会像原版一样提高吞吐；排队会放大尾延迟。get_status/list 不走这一 HSM 修改边界，表中应单独看待，短测试中的小幅波动不代表稳定收益。

## 在线文件读写

4 KiB 每组写 128 个文件、读 384 次；64 MiB 每组写 64 个文件（4 GiB）、读 192 次（12 GiB）。大文件表来自独立加长测试，包含 1 与 4 并发；不运行元数据套件和策略扫描，保留同样预热及全新集群。读写均包含打开与显式 complete，写完整 payload，读取校验字节数及每块首尾标记。文件均由普通 SDK 创建；没有 import、归档、Release 或 Recall，也未启动 hsm-rs controller、data-mover 或归档后端。该阶段测量存储侧和 SDK 自动纳管、读取保护的成本。

| 操作 | 并发 | 原版 | 新版 HSM 关闭 | 新版 HSM 开启 | 开启相对原版 |
|---|---:|---:|---:|---:|---:|
| 写入并关闭 4 KiB | 1 | 885.2 ops/s | 918.8 ops/s | 61.4 ops/s | -93.1% |
| 完整读取并关闭 4 KiB | 1 | 1,420.4 ops/s | 1,192.1 ops/s | 81.3 ops/s | -94.3% |
| 写入并关闭 4 KiB | 16 | 3,534.6 ops/s | 3,347.5 ops/s | 65.2 ops/s | -98.2% |
| 完整读取并关闭 4 KiB | 16 | 4,722.6 ops/s | 4,398.8 ops/s | 93.6 ops/s | -98.0% |
| 写入并关闭 64 MiB | 1 | 570.0 MiB/s | 539.6 MiB/s | 411.4 MiB/s | -27.8% |
| 完整读取并关闭 64 MiB | 1 | 788.5 MiB/s | 689.2 MiB/s | 684.3 MiB/s | -13.2% |
| 写入并关闭 64 MiB | 4 | 1,717.7 MiB/s | 1,533.1 MiB/s | 1,165.5 MiB/s | -32.1% |
| 完整读取并关闭 64 MiB | 4 | 2,957.4 MiB/s | 2,656.5 MiB/s | 2,303.3 MiB/s | -22.1% |


| 64 MiB 操作（4 并发） | 原版三轮范围 MiB/s | HSM 关闭范围 | HSM 开启范围 |
|---|---:|---:|---:|
| 写入并关闭 64 MiB | 1,564.3—1,796.6 | 1,258.8—1,739.2 | 978.2—1,193.9 |
| 完整读取并关闭 64 MiB | 2,857.1—3,069.9 | 2,609.8—3,027.4 | 2,120.7—2,648.1 |


大文件专测扩大到每组 4 GiB 写入/12 GiB 读取，降低原始短测的瞬时波动；三轮范围仍用于辨认重叠，不能把小幅差值当作稳定收益或生产 SLO。64 MiB 顺序读发生在刚写入之后，属于可命中 OS cache 的在线读取；不能把数 GB/s 当磁盘冷读速度。吞吐包含每文件固定的协调成本，数据块仍走普通 Worker 流；不能据此推断 TB 级长流吞吐。4 KiB 更受每文件 create/open/close 固定开销影响。

HSM 关闭也不保证完全零开销：新版 SDK 的 open 仍先调用一次 HSM control，得到 unsupported/normal 后再走普通查询；原版 SDK 无此额外往返。不能把 off 组当作删除 HSM 补丁后的原版。除已确认的 open 额外 RPC 外，其余 off 差值本轮未做因果分摊，仍需进一步 profiling；不能用能力探测解释所有读写差值。

| 操作（16 并发） | 原版 P99 | HSM 关闭 P99 | HSM 开启 P99 |
|---|---:|---:|---:|
| 创建并关闭空文件 | 3.242 ms | 3.110 ms | 195.820 ms |
| get_status | 1.366 ms | 1.535 ms | 1.466 ms |
| 打开并关闭空文件 | 1.358 ms | 2.120 ms | 183.899 ms |
| 列出 256 个子项 | 10.213 ms | 9.789 ms | 10.017 ms |
| rename | 1.690 ms | 1.827 ms | 81.951 ms |
| delete | 1.638 ms | 1.563 ms | 87.603 ms |
| 写入并关闭 4 KiB | 7.127 ms | 7.959 ms | 261.145 ms |
| 完整读取并关闭 4 KiB | 9.814 ms | 5.773 ms | 208.346 ms |


64 MiB 加长测试有 64 次写/192 次读，仍不将分位数视为生产 P99 验收。短测的 8 次写/24 次读原始结果也保留，但未用于正文大文件吞吐表。

## 周期扫描对前台的影响

on 组另以普通 SDK 预置 2048 个空文件，随后在同一规模、同一 HSM 模式下先测不运行扫描，再测运行扫描的 256 文件元数据套件。策略 min_size=u64::MAX、无容量触发限制，确保零匹配、无 seal/入队/搬运；每 RPC 最多 256 项，完整一轮后等待 1 秒再扫描。使用实际 policy_put/policy_scan RPC；与前台并发，未调用直接 RocksDB 灌库接口。

| 前台操作（16 并发） | 不运行扫描 ops/s | 运行扫描 ops/s | 变化 | 不扫描 / 扫描 P99 ms |
|---|---:|---:|---:|---:|
| 创建并关闭空文件 | 101.5 | 97.2 | -4.2% | 188.231 / 177.522 |
| get_status | 13,086.0 | 13,150.0 | +0.5% | 1.496 / 1.451 |
| 打开并关闭空文件 | 97.5 | 92.9 | -4.7% | 174.749 / 202.632 |
| 列出 256 个子项 | 1,636.2 | 972.1 | -40.6% | 10.142 / 18.148 |
| rename | 205.4 | 193.4 | -5.9% | 81.865 / 91.530 |
| delete | 207.7 | 192.3 | -7.4% | 83.747 / 102.656 |


三轮扫描统计为：

```json
[
  {
    "complete_cycles": 5,
    "examined": 11123,
    "max_page_ms": 96.070615,
    "pages": 50,
    "scan_interval_ms": 1000,
    "seconds": 8.233330761,
    "seed_files": 2048
  },
  {
    "complete_cycles": 5,
    "examined": 11059,
    "max_page_ms": 117.012141,
    "pages": 49,
    "scan_interval_ms": 1000,
    "seconds": 8.072326591,
    "seed_files": 2048
  },
  {
    "complete_cycles": 5,
    "examined": 11119,
    "max_page_ms": 95.382949,
    "pages": 50,
    "scan_interval_ms": 1000,
    "seconds": 8.469479145,
    "seed_files": 2048
  }
]
```

这是 2048 文件基础集上的扫描竞争测试，idle/active 顺序固定，存在缓存及历史增长影响；list 只有 64 次调用，短窗口可能恰好遇到扫描页，其下降比例不能当作所有周期的持续损失；不是百万 namespace 并发扫描的尾延迟结论。百万零匹配扫描独立实测为 1,000,002 项 / 3908 页 / 139.31 秒，见前份规模报告；那一数字使用 debug profile，不能与本表直接混算。

## 影响来源与后续优化方向

1. 元数据修改的全局串行协调、额外 Raft barrier 与强制 flush 批次，是当前每文件操作的主要结构性成本。该解释来自 [MasterHandler](../../curvine-master/src/master/master_handler.rs)、[hsm_read_barrier](../../curvine-master/src/master/hsm.rs) 和 [SenderTask](../../curvine-master/src/master/journal/sender_task.rs)；本次没有用 tracing 精确分摊各部分耗时。
2. 受保护读取打开、关闭会更新访问观察/读取租约并等待提交，在线大文件也有固定成本。原版纯查询式 open 的性能不能直接保留到当前实现。
3. Record JSON、真实 atime 候选迁移和持久控制结果增加写入与日志量；前轮差量索引优化减少冗余写，但没有移除全局 gate 或每 RPC barrier。
4. 后续应优先研究让普通修改等待自身日志的提交、而不是另发 barrier，以及在同一提交序列下安全批量化；保持 fencing/CAS 与 Release 安全后再考虑缩小串行范围。禁用 HSM 时可评估缓存能力探测。当前报告未宣称这些优化已经实现。

未测量：磁带、真实 Archive/Recall 搬运对前台的带宽竞争、百万普通客户端写入的性能验收、原版与 HSM 在百万 checkpoint 恢复上的 A/B、冷缓存物理盘和跨物理机网络上限。上一轮的百万 Record KV/RSS 优化数字也不能替代这些差值。

## 原始数据与复现

- [三轮全部结果](performance-results/runs.json)，含各项吞吐、延迟、进程资源采样。
- [加长大文件三轮结果](performance-results/runs-long-io.json)、[环境](performance-results/environment-long-io.json)、[源码指纹](performance-results/source-long-io.json)。
- [汇总与变化比例](performance-results/summary.json)。
- [构建二进制指纹与环境](performance-results/environment.json)、[源码指纹](performance-results/source.json)、[独立 fsync 探测](performance-results/disk-probe.json)。进程内资源包含三 Master、Worker 与 SDK；on 组最终还含扫描种子和额外控制历史，不能把整个进程最终 RSS 差值当作单个 Record 的常驻成本。

在当前 Curvine 工作树先构建并保存 HSM 基准二进制，再创建原版工作树，复制相同 example，复用同一 linker 路径以复用编译缓存：

```sh
TASK_CURVINE_ROOT="$PWD"
TASK_BENCH_OUT=/tmp/curvine-hsm-overhead-build
TASK_BENCH_TARGET="$TASK_BENCH_OUT/target"
mkdir -p "$TASK_BENCH_OUT"
CXXFLAGS=-g0 cargo build --locked --release --target-dir "$TASK_BENCH_TARGET" -p curvine-server \
  --example hsm_overhead --features hsm-scale-test -j 6
cp "$TASK_BENCH_TARGET/release/examples/hsm_overhead" "$TASK_BENCH_OUT/hsm"
git worktree add --detach /tmp/curvine-original feb3c9e20617d5c3f36c67d44033239b6ac92470
mkdir -p /tmp/curvine-original/curvine-server/examples
cp curvine-server/examples/hsm_overhead.rs /tmp/curvine-original/curvine-server/examples/
(cd /tmp/curvine-original && \
 CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="$TASK_CURVINE_ROOT/build/static-link-wrapper.sh" \
 CXXFLAGS=-g0 cargo build --locked --release --target-dir "$TASK_BENCH_TARGET" -p curvine-server --example hsm_overhead -j 6)
cp "$TASK_BENCH_TARGET/release/examples/hsm_overhead" "$TASK_BENCH_OUT/original"
python3 tools/hsm-overhead-compare.py --output /tmp/curvine-hsm-overhead-results \
  --original-binary "$TASK_BENCH_OUT/original" --hsm-binary "$TASK_BENCH_OUT/hsm" \
  --original-rev feb3c9e20617d5c3f36c67d44033239b6ac92470 \
  --hsm-rev 837b5cc4a508fc194de9a3e08dc32f00bc76674c --rounds 3
```

测量输出目录必须为空；每轮子进程结束后才启动下一轮。运行期间不应并行编译、进行其他基准或清系统缓存。大文件专测使用上面保存的两份二进制，对新的输出目录再执行同一驱动并加 `--io-only --large-files 64`，仍为 `--rounds 3`。`--smoke --rounds 1` 可缩小数据量校验流程，不能混入正式结果。
