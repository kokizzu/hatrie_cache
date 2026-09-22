# M225: Persisted Shard Leases

M225 adds an opt-in `hatReplication.ShardLeaseRegistry` for preventing two
workers from owning the same state shard at the same time. Each successful
acquisition receives a monotonically increasing fencing token. The worker
should carry that token into every state mutation or call `Validate` immediately
before a mutation.

## API

```go
registry, err := hatReplication.NewShardLeaseRegistry(
    hatReplication.ShardLeaseRegistryOptions{MaxLeases: 1024},
)
lease, err := registry.Acquire("us-east-1", "worker-a", 30*time.Second, time.Now())
if err != nil {
    return err
}
if err := registry.Validate(lease, time.Now()); err != nil {
    return err
}
// Apply the mutation with lease.FencingToken included in the storage write.
payload, err := registry.MarshalBinary()
```

`Acquire` rejects an unexpired lease, including a second acquire by the same
owner. After expiry, a takeover gets a larger fencing token. `Renew` keeps the
same token, `Release` removes the lease, and `Validate` rejects a stale owner.
The zero `MaxLeases` option uses a bounded default of 1,024 entries.

## Persistence And Recovery

`MarshalBinary` and `UnmarshalBinary` use the deterministic compact `HSL1`
format. The snapshot retains the last fencing token after a release, so a
restart cannot reuse a token that an old worker could still present. Restore is
validated before replacing live state, including duplicate shard names,
duplicate tokens, invalid identities, bounds, and trailing bytes.

The registry is process-local and does not provide a distributed consensus
protocol. The embedding service must write the snapshot atomically to its
durable store, serialize concurrent snapshot writers, protect the snapshot from
untrusted modification, and enforce `FencingToken` at the actual state-store
write boundary. A lease owner string is an identifier, not authentication.

## Measured Result

The benchmark used 256 leases on an AMD Ryzen 9 5950X, Linux amd64, Go
`-benchmem`, five samples per case. JSON is a control using the same exported
snapshot values, not a supported persistence format.

| Operation | HSL1 median | JSON median | Improvement | HSL1 memory | JSON memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Marshal | 42,673 ns/op | 119,776 ns/op | 2.81x faster | 62,040 B/op, 12 allocs | 37,058 B/op, 258 allocs |
| Unmarshal | 47,458 ns/op | 315,084 ns/op | 6.64x faster | 45,784 B/op, 522 allocs | 43,920 B/op, 529 allocs |

Snapshot size was 10,257 bytes for HSL1 and 23,991 bytes for JSON, or 2.34x
smaller. HSL1 therefore reduces persisted and transferred bytes and CPU time,
at the cost of higher marshal heap bytes and a small unmarshal heap increase in
this validation-heavy implementation. The higher heap figures are intentional
for strict duplicate/token validation and are bounded by the configured lease
count and snapshot limits.

Raw benchmark command:

```text
make benchmark-m225-shard-leases
make measure-m225-shard-lease-size
```
