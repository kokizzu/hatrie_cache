# T212: WAL Replica-Acknowledgement Retention

T212 adds an opt-in retention guard for segmented command journals. A caller
can register replica cursors and advance each cursor as that replica has
durably applied journal records. Segment pruning may remove only segments that
are safe for every registered replica.

## Default

`CommandJournalOptions.ReplicaRetentionCapacity` defaults to `0`, which keeps
the feature disabled and preserves the existing segment-retention behavior.
Set a positive capacity to enable it. The capacity is bounded by
`MaxCommandJournalReplicaRetentionCapacity` (currently 4096) so a bad peer
identity cannot grow an unbounded map.

The existing `RetainedSegments` and `RetainedBytes` policies still apply. The
replica acknowledgement is an additional lower bound: a segment is retained
when its final sequence is greater than the slowest registered acknowledgement.
The active archive is still preserved by the normal segment policy.

## Usage

```go
options := hatCache.CommandJournalOptions{
    SegmentMaxBytes:            64 << 20,
    RetainedSegments:           2,
    ReplicaRetentionCapacity:   8,
}

journal, err := hatCache.OpenCommandJournal(path, options)
if err != nil {
    return err
}

// Register before the replica starts consuming records. Zero means that no
// journal record has been durably applied by this replica yet.
if err := journal.RegisterReplicaRetention("dc-eu-node-1", 0); err != nil {
    return err
}

// Call this only after the replica has durably applied through sequence 420.
if err := journal.AcknowledgeReplicaThrough("dc-eu-node-1", 420); err != nil {
    return err
}
```

`RegisterReplicaRetention` is intentionally explicit. The existing replication
queue already reports per-target acknowledgements, but dynamic replication
targets are not silently enrolled in journal retention. An integration should
register the target when it becomes a retention participant and call
`AcknowledgeReplicaThrough` from the same durable-apply acknowledgement path.
Call `UnregisterReplicaRetention` only when the replica is intentionally
removed from the retention contract.

## Safety Rules

- Replica IDs are trimmed, must be non-empty, and are limited to 256 bytes.
- Registration and acknowledgement reject a sequence beyond the current
  journal sequence.
- Acknowledgements cannot move backwards; equal acknowledgements are
  idempotent.
- Acknowledging an unknown replica is rejected instead of silently weakening
  retention.
- When any registered replica is behind, the minimum acknowledgement protects
  all newer segments. A stalled replica therefore increases disk usage until
  it catches up or is explicitly removed.
- Cursor state is process-local and is not part of the WAL. Re-register every
  participant after restart, before new rotation/pruning work, using a
  checkpoint known to be durable on that replica.
- If the caller loses the replica's durable checkpoint, keep it registered at
  the last known safe sequence or restore it before advancing the cursor.

## Inspection

`ReplicaRetentionSnapshot()` returns sorted replica IDs with their current
acknowledged sequence, journal sequence, and lag. This is suitable for health
endpoints and disk-retention alerts.

## Measured Tradeoff

The benchmark uses 20 small journal writes, 256-byte segment rotation, and
three benchmark repetitions. Physical filesystem sync latency makes this tiny
workload noisy, so the timing values are directional only. The meaningful
cost is the extra cursor bookkeeping and the retained segments while a replica
is behind.

| Mode | Median time | Bytes/op | Allocs/op | Final segments | Relative bytes vs disabled |
| --- | ---: | ---: | ---: | ---: | ---: |
| Pre-feature baseline | 4.50 ms | 6,397 | 57 | 1 | 1.00x reference |
| Disabled (post-feature) | 4.36 ms | 6,376 | 57 | 1 | 1.00x |
| Enabled, replica lagging | 3.90 ms* | 9,924 | 85 | 19 | 1.56x |
| Enabled, replicas caught up | 4.63 ms* | 8,013 | 84 | 1 | 1.26x |

`*` The apparent timing differences are benchmark noise from filesystem
rotation and must not be treated as a performance win. Compared with the
disabled path, the lagging case uses about 56% more benchmark memory and
retains 19 times as many archived segments; the caught-up case uses about 26%
more memory and 27 additional allocations per operation. The feature is
justified by bounded replica-safe retention, not by lower write latency.

The raw runs are recorded in [BENCHMARK.md](BENCHMARK.md#t212-wal-replica-acknowledgement-retention).

## Verification

Run the focused correctness and benchmark targets:

```text
make test-t212
make benchmark-t212-before
make benchmark-t212
make cleanup-hatrie-tmp-after-test
```

The focused tests cover opt-in behavior, capacity limits, invalid IDs,
future acknowledgements, monotonic acknowledgements, and pruning only after
every registered replica has acknowledged the relevant sequence.
