# T212 WAL Retention and Replica Acknowledgements

T212 adds an opt-in retention floor for command-journal history. A caller can
register a bounded set of replica identities and feed each replica's highest
durably applied journal sequence. Segment pruning and legacy single-file
compaction cannot remove history beyond the slowest registered replica.

## Configuration

`hatCache.CommandJournalOptions.ReplicaRetentionCapacity` controls the feature:

- `0` (the default) disables the registry and preserves the existing path.
- A positive value reserves a bounded registry, up to
  `hatCache.MaxReplicaRetentionCapacity` (`1024`).
- Replica names are trimmed, must be non-empty, and are limited to 256 bytes.
- `SegmentMaxBytes`, `RetainedSegments`, and `RetainedBytes` remain the normal
  rotation and size limits. Acknowledgments add a correctness floor; they do
  not replace those limits.

```go
journal, err := hatCache.OpenCommandJournalWithOptions(path, hatCache.CommandJournalOptions{
	Format:                    hatCache.CommandJournalFormatBinary,
	SegmentMaxBytes:           64 << 20,
	RetainedSegments:          16,
	ReplicaRetentionCapacity: 3,
})
if err != nil {
	return err
}
defer journal.Close()

// Register before serving writes when the replica starts behind the journal.
if err := journal.AcknowledgeReplicaThrough("region-a", 0); err != nil {
	return err
}
```

The replication owner calls `AcknowledgeReplicaThrough` after the replica has
durably applied a sequence:

```go
if err := journal.AcknowledgeReplicaThrough("region-a", appliedThrough); err != nil {
	return err
}
```

Acknowledgments are monotone and cannot point beyond the journal's current
sequence. A new identity consumes one capacity slot. `ReplicaRetentionSnapshot`
returns a sorted detached view for status endpoints, and
`RemoveReplicaRetention` intentionally removes one identity from the floor.

## Safety and Lifecycle

The retention floor is the minimum acknowledged sequence across registered
replicas. A segmented journal removes a segment only when its final sequence is
at or below that floor and the ordinary segment limits permit removal. The
legacy single-file compaction path is capped by the same floor. The active
segment is never removed.

The acknowledgment registry is runtime state and is not serialized into the
WAL. An enabled journal starts in a protected, uninitialized state after
reopen, so it cannot prune or compact old history until the caller
re-registers at least one replica. Re-register replicas before allowing their
catch-up workflow to rely on retained history. Capacity `0` has no registry,
no map allocation, and no extra retention guard.

This API does not discover replicas, send acknowledgments, or start goroutines;
the existing replication/control-plane owner remains responsible for those
operations.

## Measured Tradeoff

Five `-benchmem` samples were collected on Linux/amd64 with an AMD Ryzen 9
5950X. The default comparison uses the same segmented-prune workload before
and after the change. Lower CPU, memory, and allocation values are better.

| Path | Raw ns/op samples | Median ns/op | Memory/op | Allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Before, existing prune | `40023 39133 40599 39309 39565` | `39565` | `15577 B` | `141` | `1.00x` |
| After, capacity 0 | `39639 39298 39675 39055 39036` | `39298` | `15046 B` | `141` | `0.99x` |
| After, eight replica floor | `40083 40135 40114 40318 38632` | `40114` | `15562 B` | `141` | `1.01x vs before; 1.02x vs default` |
| Eight-replica in-memory floor lookup | `65.77 66.32 66.04 63.76 62.65` | `65.77` | `0 B` | `0` | opt-in lookup only |

The default path has no additional allocation and did not regress in this
measurement. The enabled protected-history path costs about 2.1% CPU and 516
bytes per prune call versus the capacity-0 after path in this workload. That
cost is opt-in and buys a bounded correctness guarantee against deleting WAL
needed by a lagging replica.

Focused checks:

```text
make test-t212
make benchmark-t212-baseline
make benchmark-t212
```
