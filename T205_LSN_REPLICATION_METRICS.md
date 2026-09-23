# T205 LSN Replication Metrics

T205 adds per-target replication apply accounting to the existing source
sequence and acknowledgement lag state.

## Semantics

- `SourceSequence` is the latest journal sequence observed by the asynchronous
  replication queue.
- `LastAcknowledgedSequenceByTarget` is the greatest sequence acknowledged by
  each target.
- `ReplicationLagByTarget` is `max(SourceSequence - acknowledged, 0)`, so a
  delayed or reordered observation cannot underflow the counter.
- Apply counters are recorded when a target result is successful. A sequence is
  counted at most once per target, which prevents retries from inflating the
  totals.
- Apply throughput is acknowledgement-observed delivery throughput. It is not
  a direct measurement of receiver CPU time or storage fsync time.

## Exposed Metrics

`HTTPReplicator.MetricsSnapshot()` exposes `TargetApply` entries with:

- `LastAppliedSequence`
- `AppliedBatches`
- `AppliedEntries`
- `AppliedPayloadBytes`
- `AppliedEntriesPerSecond`
- `AppliedPayloadBytesPerSecond`

The monitoring `/metrics` endpoint exports the same values per `node` and
`target`:

- `hatrie_cache_replication_target_last_applied_sequence`
- `hatrie_cache_replication_target_apply_batches_total`
- `hatrie_cache_replication_target_apply_entries_total`
- `hatrie_cache_replication_target_apply_payload_bytes_total`
- `hatrie_cache_replication_target_apply_entries_per_second`
- `hatrie_cache_replication_target_apply_payload_bytes_per_second`

Payload bytes are the existing estimated replication payload bytes carried by
the job. They do not cause a second encoding or copy.

## Compatibility And Cost

The feature does not change commands, wire formats, storage formats, or
default replication behavior. No map is allocated until a successful,
sequenced target acknowledgement is observed. Invalid, unsequenced, and
duplicate observations are ignored.

Baseline and post-change measurements are recorded in
[BENCHMARK.md](BENCHMARK.md#t205-lsn-replication-metrics). The new observer
measured about 20.6 ns/op with zero allocations. Existing metric observation
remained about 14.2 ns/op with zero allocations, and the existing idle
snapshot remained 2,944 B with 16 allocations. One active target adds a
624-byte, two-allocation snapshot copy.

## Verification

```text
make test-t205
make benchmark-t205
make test-t205-package
make race-t205
make vet-t205
```
