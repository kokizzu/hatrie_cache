# T-U06 Replica Read-Only Gate

Status: partial, opt-in admission primitive.

`hatReplication.ReadOnlyGate` supplies a low-cost gate for external mutation
paths while allowing a trusted replication applier to continue with a permit
issued by the same gate instance. It is inspired by Tarantool replica
read-only enforcement without changing existing mutation paths by default.

## Contract

- `NewReadOnlyGate(false)` preserves writable behavior.
- `CheckMutation` must be called by every external mutation entry point that
  opts into the gate.
- `SetReadOnly(true)` atomically rejects external mutations with
  `ErrReadOnly`.
- `CheckReplication` accepts only the `ReplicationPermit` issued with that
  exact gate. Zero permits, permits from another gate, and permits used with a
  copied gate are rejected.
- The permit is an in-process capability. It must never be serialized or
  exposed to an untrusted caller.
- The gate uses one atomic word and performs no per-check allocation.

Example:

```go
gate, replicationPermit := hatReplication.NewReadOnlyGate(false)

if err := gate.CheckMutation(); err != nil {
	return err
}
// The trusted applier uses the explicit exception:
if err := gate.CheckReplication(replicationPermit); err != nil {
	return err
}
```

The package does not automatically wire the gate into every cache, SQL, HTTP,
or gRPC mutation path. Callers must place the check at each public write
boundary and keep the permit in the trusted replication component.

## Benchmark

Machine: AMD Ryzen 9 5950X, linux/amd64. Five `-count=5` samples from
`make benchmark-round36-readonly`.

| Path | Median time | Memory | Relative time |
| --- | ---: | ---: | ---: |
| Direct atomic flag baseline | 0.481 ns/op | 0 B, 0 allocs | 1.00x |
| Writable `CheckMutation` | 0.457 ns/op | 0 B, 0 allocs | within noise |
| Read-only `CheckMutation` | 0.489 ns/op | 0 B, 0 allocs | within noise |
| Trusted `CheckReplication` | 0.242 ns/op | 0 B, 0 allocs | below baseline |

## Verification

```text
make format-round36-readonly
make test-round36-readonly
make benchmark-round36-readonly
make race-round36-readonly
make vet-round36-readonly
```
