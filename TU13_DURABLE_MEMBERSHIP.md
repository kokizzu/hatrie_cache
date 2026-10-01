# T-U13 Durable Cluster Membership

`hatTopology.DurableMembershipLog` is an opt-in, append-only membership journal
for join and leave records. It is intended to make membership changes durable
across process restart while leaving consensus, transport, and operator
authorization with the caller.

## Guarantees

- Every change advances a strictly contiguous `uint64` generation.
- `Join` and `Leave` require the caller's exact current generation, so a stale
  leader cannot silently overwrite a newer membership decision.
- The default path calls `fsync` before a successful change returns.
- Replay rejects malformed records, generation gaps, duplicate joins, and
  leaves for unknown nodes.
- Snapshots return sorted copies and do not expose mutable journal state.
- A bounded record size and restrictive file mode (`0600`) prevent accidental
  unbounded input and broad local disclosure.

## Example

```go
log, err := hatTopology.OpenDurableMembershipLog("/var/lib/hatrie/membership.log", hatTopology.DurableMembershipLogOptions{})
if err != nil {
	return err
}
defer log.Close()

record, err := log.Join(0, hatTopology.TopologyNode{
	ID: "node-b",
	Address: "10.0.0.12:9000",
	Role: "replica",
})
if err != nil {
	return err
}
fmt.Println(record.Generation) // 1

snapshot, err := log.Snapshot()
if err != nil {
	return err
}
fmt.Println(snapshot.Generation, len(snapshot.Nodes)) // 1 1
```

On restart, open the same path. The journal is replayed before the handle is
returned. Keep the journal with the membership backup; restoring only a
topology JSON file without its generation history loses the stale-writer
fence.

## Durability Choice

`UnsafeNoSync: true` skips the per-change `fsync` and is only appropriate when
another ordered durable layer provides the same guarantee. It is not the
default and should not be used for authoritative membership state.

This journal is not a consensus implementation. A caller must collect a
quorum/lease decision, then pass the resulting expected generation to `Join` or
`Leave`. It also does not automatically update running peer daemons or shard
ownership; those remain explicit integration steps.

## Verification

The tests cover join/leave replay, stale generations, duplicate and missing
members, corrupt tails, copied snapshots, and the explicit no-sync mode.

The benchmark uses ten bounded journal operations per sample and five samples
on the repository's AMD64 test host:

| Path | Median observed | Memory | Allocations | Meaning |
|---|---:|---:|---:|---|
| In-memory map baseline | 23 ns/op | 0 B/op | 0 | No durability |
| Journal, `UnsafeNoSync` | 5.3 us/op | 1,008 B/op | 5 | Faster but unsafe after a crash |
| Journal, default fsync | ~0.70 ms/op (0.66-0.93 measured) | 1,008 B/op | 5 | Durable membership change |

The fsync path is intentionally much slower than an in-memory map. Membership
changes are control-plane operations rather than a data-plane hot loop; the
tradeoff buys restart safety and stale-writer detection. Use a higher-level
batched membership protocol when many changes must be committed together.
