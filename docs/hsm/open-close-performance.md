# Protected HSM open/close performance

The target is ordinary SDK `open()` + `complete()` throughput of at least 95% of upstream under identical conditions. Empty and 4KiB files, concurrency 1 and 16, and first-pass and six-second steady workloads are measured separately (eight cases). Ratios compare the median throughput of three fresh-process rounds on each side.

## Implementation and safety

The [fast-read protocol](fast-read-protection.md) replaces per-open/close consensus with qualified, bounded read grants and aggregated access checkpoints. Current-term ReadIndex includes local apply. Grants end no later than 119 seconds after the qualification sample; an existing namespace has a 120-second structural-write quarantine after leadership change or restore, including an empty durable reader map.

Access observations are immediately visible on the serving leader. Healthy operation targets approximately one-second persistence; new opens and new renewals backpressure if the dirty tail reaches one second. A crash may lose recent observations. Generation/publication/retirement facts and workflow outcomes retain committed semantics. Master `hsm_native_v10` and the matching SDK must be deployed together; this is not a mixed-version rolling upgrade.

Committed metadata caches avoid repeated inode/HSM decoding. Checkpoint validation borrows committed heads where possible, and known-absent overlays also support followers that never served a read. Cache entries publish only after successful atomic DB writes and invalidate on raw writes, deletes and restore. Closed-history retirement is bounded on the foreground and continues during idle background ticks; retired timestamps still fence replay.

## Workload and timing

- Upstream base: `96bd16632e496ebbcab73c6c6f13bcbcc26220b6`. Feature base before this change: `ed27bcda6aca17bc1493ffe1dc88b8c14597a0d3`.
- Release build, three Masters and one Worker in one process, actual TCP/Raft, one Master connection, UFS and short-circuit reads disabled.
- 4096 files per size, each written through the normal SDK. Every timed first pass opens each file once. A first pass is not a cold restart: metadata and OS caches may be warm after ingestion.
- Steady workloads run for six seconds, spanning multiple checkpoint cycles. Three paired rounds alternate the order of upstream and HSM.
- Throughput includes foreground operations, FIFO client close drain and the same two namespace RPCs on both sides. HSM namespace admission additionally persists the remaining access tail. Background work and final persistence are not silently excluded.
- P99 measures foreground per-operation latency. The final whole-workload drain is reported separately and included in the throughput denominator.
- Each process fully reads back one 4KiB file for checksum validation, then recursively cleans up all benchmark files. Those operations are outside open/close timing. This is not a large-file transfer or million-file capacity claim.

## Final result

All eight median throughput ratios passed after all runtime review fixes and strict formatting. The minimum ratio is **100.6%**, with zero errors across 48 timed results. This final build includes the shared open/close/renew cache-capacity fix.

| File / workload | Concurrency | Upstream ops/s | HSM ops/s | HSM/upstream | P99 upstream/HSM ms | HSM drain ms |
|---|---:|---:|---:|---:|---:|---:|
| Empty / first pass | 1 | 6,525.9 | 6,562.9 | **100.6%** | 0.189 / 0.566 | 85.3 |
| Empty / first pass | 16 | 13,957.6 | 16,185.8 | **116.0%** | 1.422 / 1.369 | 100.8 |
| Empty / steady | 1 | 6,754.7 | 7,536.3 | **111.6%** | 0.199 / 0.471 | 50.5 |
| Empty / steady | 16 | 14,709.9 | 27,102.7 | **184.2%** | 1.438 / 1.463 | 94.6 |
| 4KiB / first pass | 1 | 6,576.3 | 6,650.1 | **101.1%** | 0.198 / 0.444 | 82.9 |
| 4KiB / first pass | 16 | 12,189.2 | 15,504.6 | **127.2%** | 1.500 / 1.397 | 96.3 |
| 4KiB / steady | 1 | 6,199.8 | 7,035.2 | **113.5%** | 0.215 / 0.510 | 51.2 |
| 4KiB / steady | 16 | 12,968.2 | 24,809.4 | **191.3%** | 1.557 / 1.560 | 101.7 |


Individual runs still vary: the first-pass 4KiB C1 paired ratios were 100.70%, 92.11% and 113.51%. The acceptance criterion is the predefined ratio of three-round medians. Per-round throughput ranges, foreground latency and full persistence drain are retained in the archived numeric results.

Individual rounds can fall below the target even when all eight median ratios pass. Single-concurrency P99 can remain higher than upstream. The result is evidence for this workload on a shared virtual machine, not a production hardware guarantee or statistical-significance claim.

## Verification

The final reviewed source passed **426 tests**: 327 Master unit tests, five Master integration tests, 25 RPC tests, seven Raft read/recovery tests, 56 client-core tests, one wire round-trip test, two suspend-aware clock tests and three real TCP/Raft cluster tests. Two existing Master tests remain ignored.

`make format` passed its full default-workspace, all-targets Clippy gate with `-D warnings`, and client/API dependency-boundary checks passed. An additional feature-enabled Clippy build covers the `hsm_overhead` example. The cluster test really waits beyond the 120-second protection horizon; it does not shorten the safety interval for test speed.

The latest RPC regression covers heartbeat, block inventory and replication-result reporting over the shared worker-client path, plus the eight-slot reserve bound under a stalled ordinary reply. The Master regressions exercise actual open/close/checkpoint apply and validate the three read-observation fixes.

The local review findings and their regression checks are recorded in the [review report](review-open-close.md). In particular, heartbeat admission stays independent of exhausted ordinary-client admission, checkpoint publication cannot hide a newer observation tail, expired reader rows do not permanently consume capacity, and new renewals obey observation-persistence backpressure.

## Reproduction and evidence

The [benchmark harness](benchmarks/open-close/hsm_fast_read_stages.rs) and [paired driver](benchmarks/open-close/compare_fast_read_stages.py) are the same workload for both builds. To build, temporarily copy the Rust harness into `curvine-server/examples/hsm_fast_read_stages.rs` and temporarily add an empty `benchmark-hsm = []` feature in that package. Build HSM with:

```bash
cargo build --locked --offline --release -p curvine-server \
  --example hsm_fast_read_stages --features benchmark-hsm
```

Copy the binary to a stable path, then remove the temporary example and feature. Build the same harness against the upstream revision, without that feature and with a separate Cargo target directory. The feature only selects the new SDK's actual close drain and untimed metric capture. It does not relax production checks or enable `hsm-scale-test`.

```bash
python3 docs/hsm/benchmarks/open-close/compare_fast_read_stages.py \
  --upstream /path/to/upstream-binary --hsm /path/to/hsm-binary \
  --output /tmp/curvine-open-close-new-run --rounds 3 --files 4096 --seconds 6
```

The output directory must not exist. Each new run writes its actual cluster configuration, log and report; the driver writes binary hashes, all per-round results and median summary. Archived numeric evidence for this change is in [summary.json](benchmarks/open-close/summary.json), [results.json](benchmarks/open-close/results.json) and [manifest.json](benchmarks/open-close/manifest.json). Archived binary hashes identify the measured build; binaries and machine-local log/database directories are not committed.

## Historical measurements: 2026-10-23

Before the committed-cache and bounded-history follow-up, the same workload's ratios were:

| File / workload | C1 HSM/upstream | C16 HSM/upstream |
|---|---:|---:|
| Empty / first pass | 96.1% | 93.3% |
| Empty / steady | 101.3% | 162.8% |
| 4KiB / first pass | 88.8% | 104.7% |
| 4KiB / steady | 102.6% | 168.3% |

These are historical results, not current acceptance. The earlier [RPC-only measurements](rpc-concurrency.md) used a different durable-read protocol and smaller metadata workload; do not combine their absolute throughput with the final benchmark.
