# Protected fast reads

Ordinary SDK opens on an HSM volume use `read_mode=fast_v1` when the client has a suspend-aware monotonic clock. Closed, online files receive a protected handle without committing one Raft command per open or close. Offline recall and old HSM clients retain the durable read-command path.

This mode trades failover write availability for read throughput. Access observations become visible on the serving Leader before its open reply. A periodic checkpoint persists them; healthy operation targets a sub-second persistence interval, and new grants apply backpressure if an unpersisted observation reaches one second. A crash may lose those observations. File generations, publication, retirement facts and workflow outcomes retain committed semantics.

## Authority and recovery

A successful current-term ReadIndex, including local apply, qualifies a bounded positive-open horizon. The sample starts **before** the ReadIndex request. Qualification is refreshed about every 250 ms while accesses are active; a cached sample is usable for at most one second. Each grant ends at or before 119 seconds from its qualification sample, including qualification latency. Refreshing the sample never extends an earlier open receipt.

This cached proof is specific to protected positive opens. Other metadata queries and structural writers continue to qualify through ReadIndex. A missing, writing or offline target does not obtain a fast grant.

Every term change or metadata restore resets the local producer identity, proof and observations. An existing namespace enters a global 120-second recovery quarantine, even when its durable reader map is empty. During quarantine, stable namespace and lifecycle mutations are refused. The interval begins when that term's read clock is initialized, so an idle cluster can start the interval on its first qualified request. Online fast reads can proceed against the qualified committed prefix.

A newly created namespace can skip this wait only after the initiating Master receives the exact namespace storage/incarnation it proposed, in the same Raft term and without a local restore/reset. Replay, snapshot loading and observing another initializer's namespace never establish freshness.

All stable mutation owners acquire exclusive projection authority, qualify the prefix and persist the observation tail **before** preparing a change. They retain authority through committed apply or fencing. This includes native command batches, compact subtree operations, legacy mutation batches, lifecycle publication after Worker I/O, scan admission and scheduled metadata writers. Worker I/O may release authority only under the existing committed reservations; reentry qualifies and flushes again.

Live reclamation checks include a conservative one-second clock-rate margin. Compact namespace commands capture that conservative check time. Historical journal clock projections keep their original evaluation rules. The lease protocol assumes cooperative SDKs and bounded monotonic-clock rate error; the one-second margin covers independent clock-rate errors up to ±1000 ppm for these horizons. Fast SDK deadlines use Linux `CLOCK_BOOTTIME`, including host suspend, and are checked before cached reads and after data RPCs. Unsupported clocks use the legacy protocol and its existing client contract.

## Format activation and replay

An HSM-enabled Master opens the `hsm_native_v10` RocksDB format family. Older binaries cannot reopen this database. Do not remove that family to force a downgrade.

Before the first fast grant, `hsm-fast-read-activation-v1` must commit and apply. It binds activation to the namespace storage/incarnation. This prevents a successful volatile grant from preceding the replicated format fence: older state machines cannot silently apply that command and continue as if fast protection did not exist. Upgrade the Masters together; mixed-version serving is not a supported deployment mode.

`hsm-access-checkpoint-v1` persists absolute access states in sorted, bounded groups of at most 2048 rows and 1 MiB. Every row checks its durable version and generation; the entire command checks the preceding producer/sequence/hash frontier. A stale command writes nothing. An exact duplicate acknowledges its existing receipt without writing again, and a rebound identity conflicts.

Each background checkpoint turn captures a fixed set of dirty inode IDs and visits each at most once. New observations remain for a later turn; the writer does not chase a continuously changing tail. A rotating cursor bounds foreground interference while preventing starvation.

Committed access-only apply holds a shared namespace guard and a separate per-store apply mutex. It changes only RocksDB access projections and owned inode atime values; it does not mutate namespace pointers or file generations. Structural apply and restore retain the exclusive namespace guard. Protected reads can therefore continue during checkpoint validation and atomic publication. Leader, follower and recovery dispatch use the same path.

The leader observation cache enforces its entry bound for opens, closes and renewals that reload an evicted inode. Dirty entries are never evicted. If no clean slot is available, close or a new renewal returns backpressure without changing its grant or receipt, so redelivery can succeed after persistence catches up.

A checkpoint acknowledgement advances only the captured durable prefix. Opens and closes accepted while it was in flight remain in the local tail. A checkpoint cannot restore an earlier reader set over a later close. Unknown submissions are fenced and reconciled against the committed frontier before authority is released.

The existing compact namespace v5 `ReadTime` projection and legacy read-batch envelopes are unchanged. New fast replies negotiate `read_encoding=protobuf_v1`; only the transport changes. They reuse ordinary protobuf file/block layouts and carry the protected generation, reader identity, private endpoint and deadline. JSON control callers retain JSON replies.

## Reader lifecycle and compatibility

The serving Master first binds a client to its random producer identity. Binding alone grants no access. Successful reader keys contain both the producer and caller nonce. An old open packet after a term change receives only a new binding; an old renewal is fenced. Within an epoch, duplicate opens retain their original deadline, closed readers cannot reopen, and repeated renewal identities do not extend their original renewal receipt. Closed-history retirement visits at most 128 entries on an open and 1024 entries per idle background tick. Retired identities are covered by the same monotonic request-timestamp floor; active receipts remain eligible for idempotent replay before that floor check. Retired request timestamps and bounded history prevent old packets from becoming new grants.

Fast readers use owned data I/O, disable detached prefetch and bypass local short-circuit reads. `complete()` stops the local reader, makes it terminal and enqueues an advisory close. The bounded queue holds 4096 closes and sends at most 128 per control request, with a 10 ms flush interval. A dropped reader attempts best-effort close delivery. A lost close leaves protection in place until expiry.

Normal mutations through the same `FsContext` flush preceding closes before obtaining a native mutation ticket. Call `flush_hsm_read_closes().await` before graceful shutdown or handing completion to a different filesystem context or process. Its FIFO receipt covers earlier queued and in-flight closes. Repeated delivery failure is explicit and causes later flushes to fail; it never reports that an undelivered close released protection.

`GetBlockLocations.metadata_only=true` explicitly requests layout inspection and grants no read authority. Updated SDK metadata queries and transfer planning retain this operation. An old bare `GetBlockLocations` request against managed data is rejected so that an unaware SDK cannot obtain its default unprotected reader. Use `CurvineFileSystem::open` for managed data reads. Published private allocations continue to use their protected private Worker endpoint, rather than a native block lookup.

UnifiedFS cache readers use `get_unmanaged_read_blocks` with `metadata_only=false`, retaining one layout RPC for an ordinary cache open. With HSM enabled the Master returns typed `Unsupported` for managed records and native regular files without UFS backing, including files not yet enrolled. UnifiedFS propagates this denial even if its cached mount route still points to an old UFS generation. Explicitly wrapping a managed reader in `FallbackFsReader` is also refused before reading cached bytes.

UFS mounts cannot overlap managed HSM data or the reserved HSM namespace. New mounts and updates are checked before publication; restored and historically replayed mounts are checked before external routing is returned. An incompatible persisted mount makes the mount-table RPC fail until an operator unmounts it; unmount remains available. Choose an unmanaged subtree for UFS caching. Ordinary unmanaged UFS-backed files and HSM-disabled operation retain their existing behavior.

`GetCvMetadataSnapshotPage` also uses qualified metadata admission when HSM is enabled. Its blocking traversal owns the read ticket while queued and running, including after RPC cancellation. Page tokens remain opaque path strings, compared by path components to match the name-sorted depth-first traversal; punctuation and multibyte sibling names cannot disappear between pages. Each page has its own qualified read; pagination does not freeze concurrent namespace changes across multiple requests.

Snapshot pages retain offline and private-layout file status with no ordinary block layout (`blocks=None`). Their presence does not invoke an ordinary data read or prevent enumeration of the rest of the namespace. Database errors and errors resolving an ordinary native layout remain explicit.

## Metadata lookup cost

Protected opens cache derived block-to-worker mappings in a bounded 65,536-block LRU. A committed location update refreshes just its changed block IDs from RocksDB before the writer returns; unaffected mappings remain cached. Uncommitted batches leave the cache intact. Worker availability and endpoints are resolved against the current Worker manager on every open.

A separate 65,536-key LRU retains parsed committed HSM base records and access overlays. Typed writes serialize the same value they cache and publish it only after the WriteBatch succeeds. Raw writes invalidate the relevant parsed value, and range deletes preserve the ordering of puts and deletes within their batch. Base/access merging performs the same version and generation checks as an uncached snapshot read.

File inode values additionally use an 8192-entry committed cache, limited to encoded file values of at most 16KiB. Directory and larger values fall back to RocksDB. Callers receive owned values; editing or dropping an uncommitted batch cannot change cached committed metadata. Raw inode writes and deletes invalidate the affected entries, and successful typed batches publish the exact values they serialized.

Access checkpoint preflight can borrow just the committed base/access head rather than clone the full record or repeat RocksDB seeks. Missing cache knowledge retains the original database path. Stable materialization records a known-absent access overlay only when the batch deletes it or the database confirms absence. It does not add tombstones or conceal an unexpected overlay; version and generation validation remains identical on both paths.

Committed-projection reset invalidates these caches, and snapshot restore constructs a new store and caches. Concurrent misses carry a commit epoch so a lookup cannot repopulate a cache after an intervening invalidation. These caches do not grant read authority or replace generation checks.

Recovery flushes its full-schema range tombstones out of active memtables before replay. Aggregate access apply reuses one iterator per relevant column family and decodes only the fields required for its conditional checks. Iterator reads continue to respect range tombstones and propagate relevant corruption.

## Validation

The real three-Master/one-Worker test `fast_grants_survive_leader_transfer_without_a_durable_reader_row` pauses access checkpoints, grants readers and transfers Raft leadership. It checks that the successor refuses deletion despite an empty durable reader map, rejects old renewal identities, and allows the original data to be read during protection. After waiting beyond the full recovery horizon, cached SDK reads must fail before deletion can succeed.

The mixed integration test covers duplicate opens/renewals/closes, overwrite rejection across cache invalidation, ordinary native reads, private imported bytes, subtree operations, physical GC, controller takeover, off-gate Worker I/O and accepted-operation cancellation. Unit tests cover aggregate atomicity, stale frontiers, replay/idempotency, format activation, schema downgrade rejection, clock reset/freshness and suspend-aware client expiry.

Performance evidence is recorded separately with exact binaries, source hashes, configuration and raw reports. The paired harness uses one Master connection and includes FIFO close drain plus the same two namespace RPCs on both sides; HSM namespace admission also persists the remaining access tail. Its single-pass and steady-state results are reported separately.

## Measured throughput (2026-10-23)

Three paired fresh-process rounds used 4096 files, six-second steady loads, three Masters, one Worker, one Master connection and no short-circuit I/O. Throughput includes FIFO close drain and the final namespace operations that persist the remaining access tail.

| File | C1 HSM/upstream | C16 HSM/upstream |
|---|---:|---:|
| Empty | 101.3% | 162.8% |
| 4KiB | 102.6% | 168.3% |

The first-pass 4KiB C1 case, including complete persistence, reached 88.8% and remains below the 90% target. The first pass uses freshly ingested files with potentially warm metadata/OS caches; it is not a cold restart. Foreground P99 remains above upstream. Exact results, limitations and raw artifacts are in the [performance report](open-close-performance.md#historical-measurements-2026-10-23).

## Throughput follow-up (2026-10-26)

Committed file inode caching, borrowed checkpoint heads, follower-safe known-absent overlays and bounded closed-history retirement raised all eight three-round median throughput ratios above the 95% target. The same one-connection benchmark includes full tail persistence for first passes and six-second steady loads.

| File / workload | C1 HSM/upstream | C16 HSM/upstream |
|---|---:|---:|
| Empty / first pass | 100.6% | 116.0% |
| Empty / steady | 111.6% | 184.2% |
| 4KiB / first pass | 101.1% | 127.2% |
| 4KiB / steady | 113.5% | 191.3% |

These are the final post-review ratios of three-round medians; individual rounds still vary. Single-concurrency foreground P99 remains above upstream. The read horizon, failover quarantine, observation backpressure and commit requirements are unchanged. Full results, test evidence and source hashes are in the [95% report](open-close-performance.md).
