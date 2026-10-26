# HSM fast-read review (2026-10-26)

Scope: the complete working-tree implementation based on `ed27bcda6aca17bc1493ffe1dc88b8c14597a0d3`, including the RPC concurrency layer, protected SDK lifecycle, read qualification, access checkpoint apply, committed caches, wire changes, tests and upgrade documentation. The destination is the existing `feat/hsm-adapter` branch of the author's fork.

Intent: ordinary HSM SDK open/complete throughput of at least 95% of same-configuration upstream, while retaining data protection, idempotency and failover safety. The approved access-observation tradeoff permits approximately one second of observations to be lost on crash; generations, publication, retirement and workflow results remain committed.

## Standards

The review used CONTRIBUTING.md, the Curvine workflow and manual review rules, and existing dirty-crate contracts. Tool-enforced style issues were left to the formatter and Clippy. No optional code-smell concern warranted a finding.

| Finding | Evidence and impact | Resolution |
|---|---|---|
| P2: heartbeats share saturated client admission | A stalled ordinary reply can retain every global permit for the socket write timeout, preventing healthy WorkerHeartbeat from reaching its actor runtime and causing worker blacklisting. | A default-false MessageHandler classification selects a separate server-wide eight-slot bounded liveness reserve. MasterHandler enables it for WorkerHeartbeat, WorkerBlockReport and ReportBlockReplicationResult: incremental reports block the heartbeat loop, and replication results reuse its MasterClient. Per-connection bounds and permit-through-send ownership remain. TCP regression holds ordinary admission at one, proves heartbeat/inventory/replication-result/heartbeat progress, and verifies the ninth reserved request waits. |
| P2: close/renew reload bypasses the observation-cache bound | A clean cache entry for a still-active grant can be evicted. Closing or renewing that grant reloaded the inode without checking capacity, allowing repeated cycles to grow the leader cache past its limit. | Open, close and new renewal mutations share bounded slot admission. Reloading evicts only clean entries; if all entries are dirty it returns Busy before changing the grant or renewal receipt. Regressions cover capacity eviction, all-dirty rejection, exact receipt replay and successful idempotent retry after an acknowledged prefix becomes clean. |
| Documentation evidence links leave the repository | The new protocol docs link to workspace-parent reports, which are unavailable in a GitHub checkout. | The [performance report](open-close-performance.md), reproducible harness, sanitized binary manifest and numeric round results are versioned under docs/hsm. Relative links point to committed files. |

The liveness guarantee applies to independent worker connections; it does not bypass admission already blocked on the same connection or promise progress after all liveness reserve slots themselves are occupied. Block and replication reports share the same bounded reserve; otherwise a report could prevent the next worker heartbeat through the synchronous loop or shared connection. Actor-runtime routing remains unchanged.

## Spec

| Finding | Requirement and concrete trigger | Resolution |
|---|---|---|
| P2: apply-before-ack hides newer observations | The protocol promises leader-local observations before a successful open reply. After a captured checkpoint publishes but before its waiter acknowledges, resolve/inspect could ignore a newer open/close tail because the durable version lagged the DB version. | Overlay matching accepts either the original durable prefix or the exact captured flight prefix with the same DataKey. A deterministic test publishes a real checkpoint, admits an open/close tail before acknowledgement, and checks both visibility and negative generation/prefix cases. |
| P2: expired readers permanently consume the 1024-reader budget | Lost closes must leave protection only until expiry. Admission checked capacity before pruning expired rows, so a full persisted reader map stayed unreadable after all readers expired. | Conservative expiry pruning runs before counting capacity. Regression covers 1024 protected rows, one safely expired slot, all expired rows, and a raw-expired row still inside the clock margin. |
| P2: new renewals bypass dirty-tail backpressure | New grants must backpressure once the unpersisted tail reaches one second. New renewal identities could continue extending protection and mutating observations during an indefinite checkpoint stall. | New renewal mutations perform the same dirty-age admission check. Exact renewal receipt retries remain idempotent. Regression confirms rejection does not change version or stored renewal identity. |

The review found no additional concrete defect in pre-ReadIndex horizon anchoring, term/restore producer fencing, activation-before-grant ordering, 120-second recovery quarantine with empty reader maps, aggregate frontier CAS, fixed-prefix acknowledgement or ambiguous-submission reconciliation. Structural/background mutation owners and Worker-I/O reentry were traced to qualified exclusive admission and access-tail draining.

Wire changes append optional tags. Old bare block-location requests are intentionally rejected for managed reads, and the coordinated Master/SDK migration is documented in the [protocol](fast-read-protection.md). The caches are derived metadata and do not replace read authority or generation checks.

## Validation and verdict

- 426 tests passed on the final source, including 327 Master unit, five Master integration, 25 RPC, seven Raft read/recovery, 56 SDK, one wire, two clock and three real cluster tests. Two existing Master tests remain ignored.
- Full `make format` default-workspace all-targets Clippy with `-D warnings` passed; client/API dependency-boundary checks passed. The feature-enabled benchmark example also passes Clippy.
- The new regressions were reviewed against their concrete failure modes. Real failover still waits beyond the complete 120-second protection horizon, rejects early deletion with no durable reader row and rejects cached SDK data after expiry.
- The post-review fixed-binary three-round benchmark passed all eight median ratios, minimum 100.6%, with 48 zero-error timing results. [Results and reproduction](open-close-performance.md).

Final verdict: Approve.

Standards: three findings resolved, no remaining blocking finding. Spec: three findings resolved, no remaining blocking finding. The worst original issue in each axis was P2; no axis was used to mask the other.
