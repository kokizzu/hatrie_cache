# TT-006: Hot-Standby WAL Catch-Up

Status: partially adopted.

This adopts Tarantool-style read-only hot-standby promotion as an importable,
transport-neutral state machine. Snapshot installation, WAL transfer, record
application, fencing, and topology publication remain caller-owned. The
coordinator only validates the lifecycle and publishes detached state.

## Lifecycle

1. `Start` begins from an already-installed snapshot sequence and binds the
   standby to one primary ID, source term, storage generation, and fencing
   token.
2. `ObserveHead` records a monotonic primary WAL head. The configured lag
   bound rejects a source that is too far ahead for the standby's recovery
   envelope.
3. `ApplyWAL` accepts only contiguous batches whose end was observed. Replays
   that are already fully applied are idempotent; gaps and partial overlaps are
   rejected.
4. The state becomes `CaughtUp` only when applied and advertised sequences are
   equal.
5. `Promote` requires the current generation, fencing token, and exact
   catch-up, then requires a strictly newer term before entering `Primary`.

The coordinator stores no WAL records, so its memory is constant with respect
to the stream. Callers must fence the old primary and atomically publish the
new topology around `Promote`.

## Defaults And Limits

- The feature is default-off: constructing `HotStandbyCoordinator` does not
  start networking, replay, or promotion.
- `MaxLag: 0` selects `1,048,576` WAL sequence numbers.
- Configured lag is bounded to `2^32` sequence numbers.
- Node identifiers are trimmed and limited to 256 bytes.
- Terms, fencing tokens, and storage generations must be non-zero.

## Benchmark

Five `-benchmem` samples ran on Linux amd64 with an AMD Ryzen 9 5950X. The
baseline is raw local integer bookkeeping; the validated path includes a
state snapshot plus the locked, fenced head and batch transitions. This is a
control-plane safety-cost comparison, not a claim that local integers provide
equivalent correctness.

| Path | Raw ns/op samples | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Raw local bookkeeping | 0.5635, 0.5432, 0.5593, 0.5447, 0.5488 | 0.5488 | 0 | 0 | 1.00x |
| Validated hot-standby replay | 66.66, 66.97, 63.26, 65.48, 62.63 | 65.48 | 0 | 0 | 119.3x slower |

The absolute validated cost is 65.48 ns/op with no heap allocation. It is not
added to ordinary writes because the API is not wired into the existing write
or network paths. The tradeoff is intentional: a caller gets monotonic WAL
and promotion-fencing checks without retaining the WAL or silently promoting a
stale standby.

## Verification

Focused, package, race, and vet targets are defined in the `Makefile` under the
`tt006-hot-standby` names. Tests cover normal replay and promotion, stale
fences, lag limits, gaps, early promotion, duplicate replay, malformed plans,
and the maximum sequence boundary.
