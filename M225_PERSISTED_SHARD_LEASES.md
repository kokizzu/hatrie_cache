# M225: Persisted Shard Leases

`SQLShardLeaseRegistry` is an opt-in ownership primitive for state shards. It
allows one live owner per shard, gives every takeover a larger fencing token,
and exposes a bounded binary checkpoint for caller-owned durable persistence.
It does not start a renewal goroutine, write files, or provide consensus.

## API

```go
registry, err := hatSql.NewSQLShardLeaseRegistry(
	hatSql.SQLShardLeaseRegistryOptions{
		LeaseDuration: 30 * time.Second,
	})
if err != nil {
	panic(err)
}

lease, err := registry.Acquire("region-eu/shard-7", "worker-42")
if errors.Is(err, hatSql.ErrSQLShardLeaseHeld) {
	// Another worker still owns the shard.
}

lease, err = registry.Renew(lease)
if err != nil {
	// The lease expired or a newer fencing token replaced it.
}

checkpoint, err := registry.MarshalBinary()
if err != nil {
	panic(err)
}
snapshot, err := hatSql.UnmarshalSQLShardLeaseSnapshot(checkpoint)
if err != nil {
	panic(err)
}
if err := registry.Restore(snapshot); err != nil {
	panic(err)
}
```

`Acquire` rejects every unexpired lease, including one with the same owner ID;
renewals must use the exact returned `SQLShardLease` value. When a lease
expires, the next successful acquisition receives a larger `FencingToken`.
`Renew` and `Release` reject an older owner/token pair, so delayed cleanup from
a previous worker cannot remove a replacement lease.

The fencing token is only useful when the downstream state write carries and
checks it. A state store should accept a write only when its token is at least
the stored token, and should reject lower tokens. The registry alone cannot
stop a worker that ignores this check.

## Persistence Workflow

1. Construct the registry and restore the last committed snapshot before
   starting state workers.
2. Persist `MarshalBinary()` atomically with the ownership metadata. The
   checkpoint is caller-owned; use the same durable transaction/WAL discipline
   as the state it protects.
3. Use a stable, unique owner ID per worker instance and renew substantially
   before `LeaseDuration` expires.
4. Include `FencingToken` in every state mutation. A crashed worker is then
   fenced after another worker takes over.
5. Treat `Release` as best-effort cleanup. Expiry and the next fencing token
   are the recovery path when a worker disappears.

`Restore` removes records already expired according to the configured clock,
but always retains `NextFencingToken` so a restored registry never reuses an
older token. The binary `HSL1` checkpoint is length-bounded and CRC32 checked
for accidental corruption. CRC32 is not authentication; protect checkpoints
with an authenticated storage channel when an attacker can modify them.

## Defaults And Tradeoffs

The registry is disabled unless a caller constructs it. Defaults are 16 lock
shards, 65,536 retained shard records, a 30-second lease, 256-byte shard IDs,
and 128-byte owner IDs. All limits can be lowered, and the ID bounds are capped
at 4,096 bytes.

Normal ownership operations use per-lock-shard maps and make no heap
allocation in the benchmarked path. Checkpointing copies and sorts active
leases, so its cost grows with the number of owned shards. The feature adds no
cost to existing SQL execution or materialized-view workers until they opt in.
