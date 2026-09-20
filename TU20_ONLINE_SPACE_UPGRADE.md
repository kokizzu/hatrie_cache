# T-U20 Online Space Upgrade

`OnlineSpaceUpgrade[K, V]` is an opt-in generic space for online format
conversion. It keeps per-record versions, writes new records in the target
format while an upgrade is active, lazily converts old records on reads, and
offers bounded background batches before an atomic cutover.

## Workflow

1. Create the space at its current schema version.
2. Store normal values with `Set`.
3. Call `Begin(targetVersion, converter)`.
4. Continue normal reads and writes. Writes use the target version; reads of
   old records convert atomically and leave failed conversions untouched.
5. Run `UpgradeBatch(limit)` from a worker until `PendingRecords` is zero.
6. Use `Pause`/`Resume` for operational throttling and call `Complete` only
   after all old records are converted or deleted.

```go
space, err := hatDataStructure.NewOnlineSpaceUpgrade[string, UserV1](1)
if err != nil {
	return err
}

if err := space.Begin(2, func(old UserV1) (UserV1, error) {
	return upgradeUser(old)
}); err != nil {
	return err
}

converted, remaining, err := space.UpgradeBatch(1000)
```

The value type is generic; SQL/space adapters own tuple decoding, validation,
and the actual converter. The converter must not call back into the same space
while it runs because conversion is serialized with the space mutex.

## Recovery and rollback

Each stored entry carries its version, so a caller can persist the entry data
and coordinate state through `VersionedMigrationManager`. After a process
restart, reconstruct the space at the old current version, restore entries
with their stored versions through the adapter, and call `Begin` with the same
target and converter. Already-target-version entries are skipped and remaining
old entries resume from the new scan.

The structure does not invent a reverse converter. `Complete` is therefore a
forward cutover, and rollback of application data is caller-owned. Use the
versioned migration manager for durable preconditions, dependency ordering,
mixed-version client gates, and operator rollback state.

## Safety

- Initial and target versions must be positive and strictly increasing.
- A missing converter, invalid batch size, invalid phase, or incomplete batch
  is rejected without changing the data or progress counters.
- Conversion errors leave the original value and pending count unchanged.
- Deletes remove old records from pending work; writes during a paused upgrade
  still use the target version so compatibility continues.
- `Stats` exposes current/target versions, phase, item count, pending records,
  successful conversions, and batch scan progress.

## Measured cost

Measured on Linux/amd64, AMD Ryzen 9 5950X, with five `-benchmem` samples.
The map baseline was measured separately with hot string keys.

| Workload | Map baseline | Online space | Relative result | Memory |
| --- | ---: | ---: | ---: | --- |
| Current-format hot `Get` | 7.486 ns/op | 13.28 ns/op | 1.77x slower | 0 -> 0 B/op; 0 -> 0 allocs/op |
| Current-format hot `Set` | 11.11 ns/op | 23.97 ns/op | 2.16x slower | 0 -> 0 B/op; 0 -> 0 allocs/op |
| Target-format hot `Get` during upgrade | 7.486 ns/op control | 13.39 ns/op | 1.79x versus map | 0 B/op; 0 allocs/op |
| One-record batch conversion | N/A | 281.7 ns/op | conversion plus setup | 448 B/op; 4 allocs/op |

The mutex and per-record version bookkeeping make this slower than a raw map.
The feature is justified when compatible writes, lazy reads, bounded
background conversion, and an explicit cutover are worth that cost. The batch
measurement includes creating a fresh space and one record per iteration; a
long-lived upgrade worker amortizes that setup across its batch.

Raw samples:

```text
BenchmarkTU20BeforeMapGet-32                  9.187 7.577 7.349 7.486 7.384 ns/op  0 B/op   0 allocs/op
BenchmarkTU20BeforeMapSet-32                 11.11 11.57 10.39 11.38 10.78 ns/op  0 B/op   0 allocs/op
BenchmarkTU20AfterSpaceGet-32                14.09 13.34 13.19 13.06 13.28 ns/op  0 B/op   0 allocs/op
BenchmarkTU20AfterSpaceSet-32                23.04 23.97 24.11 24.37 23.10 ns/op  0 B/op   0 allocs/op
BenchmarkTU20AfterSpaceTargetGet-32           13.39 13.19 13.77 12.46 13.52 ns/op  0 B/op   0 allocs/op
BenchmarkTU20AfterOneRecordBatchConversion-32 300.7 281.7 279.1 282.4 274.8 ns/op 448 B/op 4 allocs/op
```

Reproduce with `make benchmark-tu20-before`, `make benchmark-tu20`, and
`make verify-tu20`.
