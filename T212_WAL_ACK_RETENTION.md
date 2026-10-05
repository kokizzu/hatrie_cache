# T212: WAL Retention From Replica Acknowledgments

T212 adds an opt-in retention fence for segmented command journals. A caller
registers each replica's last sequence that is durably applied; the journal
retains every archived segment whose end sequence is newer than the slowest
registered acknowledgment.

## API

```go
if err := journal.SetReplicaWatermark("node-eu", appliedSequence); err != nil {
	return err
}

status := journal.ReplicaWatermarks()
_ = journal.RemoveReplicaWatermark("node-eu")
```

`SetReplicaWatermark` trims and bounds the replica name, rejects a sequence
past the current journal tail, and rejects regressions. `ReplicaWatermarks`
returns a sorted immutable snapshot with sequence lag. Removing a watermark is
explicit because a removed replica may need a fresh snapshot before it can
resume.

## Retention Semantics

- The effective fence is the minimum sequence across all registered replicas.
- A segment ending at or before that minimum may be removed by the existing
  segment-count or byte-budget policy.
- A segment ending after that minimum remains, even when the ordinary budget
  would remove it.
- The active journal file is never removed.
- Existing outbox and SQL projection retention fences remain effective too.
- The feature has no effect until a caller registers a watermark. Existing
  journal options and the default write path remain unchanged.

Acknowledgments must be updated only after the replica has durably applied the
sequence. The API is deliberately caller-driven so transports, replication
topologies, and lock ownership do not become coupled. In particular, the
existing `HTTPReplicator` does not automatically enable this policy.

Watermarks are process-local, matching the existing projection watermark
contract. A replication controller that persists its own checkpoints should
restore them after opening a journal and before relying on a narrow retention
budget; deployments should retain a restart-safe count/byte margin until that
restore has completed.

## Defaults And Bounds

The default is off: the watermark map is nil and the normal pruning path has
no per-replica allocation. Replica names are limited to 256 bytes. The fence
only participates when segmented retention is already configured with
`SegmentMaxBytes` and a positive retained-segment or retained-byte budget.

## Measurement

On the benchmark host, the isolated fence scan measured:

| Mode | ns/op | B/op | allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Disabled | 6.19-6.32 | 0 | 0 | 1.00x |
| One replica | 36.09-38.43 | 0 | 0 | 5.97x |
| Four replicas | 49.22-51.34 | 0 | 0 | 8.08x |

This is the opt-in fence calculation, not a claim about total command latency.
The existing segmented-prune baseline was `65.4-66.7 us/op`,
`31.1-32.2 KB/op`, and `270 allocs/op` for count-only retention. A byte-budget
control measured `174.4-177.5 us/op`, `55.9 KB/op`, and `397 allocs/op`; T212
does not add that byte-budget cost.

Focused correctness, race, and vet targets:

```text
make codex-t212-red
make codex-t212-race-focused
make codex-t212-vet
make codex-t212-bench
```
