# T043: Scalar Recovery Arena

## Summary

Recovery of the default binary command journal now has a bounded fast path for
scalar `SET`, `SETSTR`, and `SETINT` records. It borrows strings from the
already-read journal payload, copies only the fields needed for the replay
batch into a reusable arena, and materializes at most 256 records per flush.

This is inspired by ClickHouse's use of arenas and low-copy parsing for short-
lived execution state. It is deliberately limited to recovery; normal command
execution and the on-disk journal format are unchanged.

## Eligibility and fallback

The fast path is used only when all of the following are true:

- the journal uses the default binary format;
- journal idempotency is disabled;
- the command is `SET`, `SETSTR`, or `SETINT`;
- the record has no TTL, expiry timestamp, subkey, dynamic value, pair list,
  atomic flag, idempotency fingerprint, or outbox payload.

JSON journals, TTL-bearing records, idempotent journals, dynamic commands,
batch commands, outbox records, and non-scalar commands continue through the
general decoder and replay path. This keeps the optimization narrow and keeps
the existing behavior for every other record type.

The arena is bounded by `maxCommandJournalReplayScalarBatchRecords` (256).
Each flush materializes the records before the arena is reset. String values
that must survive the flush are copied; keys may borrow from the reusable
arena only until the current replay batch completes.

## Correctness and safety

The specialized decoder still validates the binary version, sequence,
checkpoint marker, field presence, command eligibility, and complete payload
consumption. Truncated or malformed payloads return errors. The borrowed
strings are read-only and are never exposed after the payload or arena is
reused. Tests cover binary decoding, truncation, multiple arena flushes, JSON
fallback, TTL fallback, the complete journal replay suite, backup/restore,
race detection, and `go vet`.

## Measurement

Benchmark: `BenchmarkT042Replay`, 4,096 scalar binary journal records per
operation, five samples per side, same machine and benchmark target.

| Metric | Before | After | Change |
| --- | ---: | ---: | ---: |
| Median CPU time | 3,117,838 ns/op | 2,368,588 ns/op | 1.32x faster |
| Median bytes allocated | 1,360,680 B/op | 441,598 B/op | 3.08x lower |
| Median allocations | 20,515 allocs/op | 8,247 allocs/op | 2.49x fewer |

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md). The change has no
new configuration switch: the optimized path is automatic for the safe
default case, with the existing decoder acting as the compatibility fallback.
