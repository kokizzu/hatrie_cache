# T208 Anonymous Replicas

`hatReplication.QuorumTarget` adds an opt-in way to fan out a read or write to
replicas that should receive the operation but must not affect quorum safety.
Set `Anonymous: true` for observers, analytics copies, delayed replicas, or
other replicas that are useful for eventual delivery but are not voting
members.

```go
targets := []hatReplication.QuorumTarget{
	{Node: "voter-a"},
	{Node: "voter-b"},
	{Node: "analytics-copy", Anonymous: true},
}

result, err := hatReplication.ExecuteWriteQuorumTargets(
	ctx,
	targets,
	2,
	write,
)
```

All three targets are attempted. `Decision.Total` is `2`, and the analytics
copy cannot satisfy the required acknowledgement threshold. The same rule
applies to `ExecuteReadQuorumTargets`: anonymous values are returned in the
per-target attempts but cannot form or satisfy a matching read-value group.

The existing `ExecuteWriteQuorum` and `ExecuteReadQuorum` functions are
unchanged and continue to treat every supplied node as a quorum participant.
The new target-aware functions reject an empty voter set, duplicate node
names, blank names, or a required threshold greater than the number of voting
targets.

## Benchmark

The benchmark was run on an AMD Ryzen 9 5950X with:

```text
go test ./hat/hatReplication -run '^$' -bench 'T208' -benchmem -benchtime=1s -count=3
```

The baseline was the clean C208 commit before T208. Values below are the
median of three runs; latency is shown for orientation, while memory and
allocation changes are the stable result to use for capacity planning.

| Operation | Baseline | Target-aware voters | Target-aware with anonymous target |
| --- | ---: | ---: | ---: |
| Write time | 1,274 ns/op | 1,303 ns/op | 1,171 ns/op |
| Write memory | 544 B/op, 10 allocs/op | 496 B/op, 9 allocs/op | 496 B/op, 9 allocs/op |
| Read time | 1,628 ns/op | 1,361 ns/op | 1,333 ns/op |
| Read memory | 864 B/op, 12 allocs/op | 816 B/op, 11 allocs/op | 784 B/op, 11 allocs/op |

The target-aware path uses 1.10x fewer write bytes and 1.06x fewer read bytes
than the legacy baseline, with one fewer allocation in both paths. It is
opt-in, so deployments that do not need anonymous replicas have no behavior
change and keep the existing APIs.
