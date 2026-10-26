# Fork main integration review (2026-10-26)

Scope: the complete `feat/hsm-adapter` difference from fork main `1e9bc427306d979c814dbf48195b37b1b1038fdc`. The reviewed feature tip before these fixes was `eb909b975a9ceff0b296485b921aa119bf22d169`. Destination: `JayTsu-sh/curvine` main. The existing upstream snapshot `Cow` change and its file/directory borrowing were included in the review.

The [fast-read review](review-open-close.md) covers the implementation and fixes already present at that feature tip. This integration review checks the complete fork-main range and records the additional findings separately. Standards sources are CONTRIBUTING.md and the repository Curvine workflow; requirements come from the HSM protection, HA and performance documents and the approved one-second access-observation recovery tradeoff.

## Standards

| Finding | Concrete trigger | Resolution |
|---|---|---|
| P2: mounted cache reads bypass managed-read protection | UnifiedFS used the inspection-only layout API to construct an unmanaged reader. A retained mount-cache entry could also route a newly managed native file to an obsolete UFS generation. | Cache reads use a purpose-specific `metadata_only=false` API. Master rejects managed records and eligible native regular files with typed `Unsupported`; UnifiedFS propagates that error. Fallback refuses managed readers before cached bytes. New and externally published restored mounts reject managed/reserved overlap; internal Native mount lookup remains lock-neutral. |

The normal unmanaged cache open still performs one layout RPC. The TCP regression retains the old mount route, makes the server deny the new managed generation, and provides real obsolete UFS bytes which must not be opened. Separate tests cover explicit managed-reader wrapping with cached bytes, pre-enrollment refusal, inspection access, valid UFS-backed reads, incompatible persisted mounts, remediation by unmount, and HSM-disabled behavior.

No remaining documented-standard violation or reportable optional code smell was found. Previous review findings remain resolved.

## Spec

| Finding | Requirement and concrete trigger | Resolution |
|---|---|---|
| P2: snapshot cursor order can omit siblings | Complete snapshot pagination must not lose entries. DFS visits `/a/z` before `/a.txt`, but whole-path lexical cursor comparison can skip `/a.txt`. | Both pruning and emission compare path components, matching the traversal. Tests compare complete results with page sizes 1/2/3 across punctuation and multibyte names. Token encoding is unchanged. |
| P2: snapshot RPC lacks committed-read qualification | HSM metadata reads must use a qualified committed prefix. The async snapshot handler could traverse during a speculative legacy mutation or without quorum. | The handler uses the existing MetadataReads ReadIndex coordinator. The actual queued blocking task owns the admitted ticket until traversal finishes, even when its RPC waiter is canceled. Tests check exclusive-turn blocking, unqualified-prefix refusal and queued cancellation ownership. |
| P2: snapshot traversal fails on non-native managed layouts | Metadata enumeration must retain offline and private-layout files. The real cluster regression found that traversal invoked the ordinary data-read guard and aborted the entire page. | Such entries keep their status and return `blocks=None`, since no ordinary native read layout is available. Record/database errors and ordinary layout errors still propagate. Unit coverage checks both offline-native and private-online records without weakening data-read guards; the real cluster RPC covers a mixed namespace. |

All three snapshot defects existed on fork main before this branch; they were found while reviewing and testing the touched subsystem and fixed in this integration. The upstream `Cow` change did not introduce them. Pagination supplies a qualified view per request, not a frozen multi-request namespace snapshot.

No remaining missing, partial or incorrectly implemented requirement was found in this reviewed range. Previous review findings remain resolved.

## Validation and verdict

- 336 Master unit tests, 56 client-core unit tests, 18 UnifiedFS unit tests and 25 RPC tests passed. Two pre-existing Master tests remain ignored.
- All three real TCP/Raft cluster tests passed on the final implementation, including the mixed snapshot RPC and typed raw-read refusal, failover with an empty durable reader map, expiry of cached data and the full recovery horizon.
- Default-workspace `make format` (all-targets strict Clippy and formatting), strict Clippy for the feature-enabled cluster test, and client/API dependency-boundary checks passed.
- All five Master integration tests passed, for a total of **443 passing tests** across these gates.

The existing [95% performance results](open-close-performance.md) remain evidence for their recorded source/binaries, not a new benchmark of this integration. These fixes do not change the timed ordinary No-UFS open/complete path.

Standards: one additional P2 resolved, no remaining blocker. Spec: three additional P2 resolved, no remaining blocker. Final verdict: **Approve**.
