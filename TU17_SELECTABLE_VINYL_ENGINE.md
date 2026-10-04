# T-U17 Selectable Vinyl-Style Space Engine

`hatSpace` now exposes a per-space engine choice:

- `EngineMemtx` is the default. It is an in-memory map with no files and the
  lowest latency.
- `EngineVinyl` is opt-in. It uses the existing checksummed
  `hatDataStructure.SpillableArrangement` to keep a bounded hot working set
  and persist records on disk.

The default path is unchanged. Calling `hatSpace.Open(hatSpace.Options{})`
selects `memtx` and does not create a storage path.

## Usage

```go
engine, err := hatSpace.Open(hatSpace.Options{
	Kind:            hatSpace.EngineVinyl,
	Path:            "/var/lib/hatrie/spaces/orders.arrangement",
	MemoryLimitBytes: 64 << 20,
	MaxDiskBytes:    512 << 30,
})
if err != nil {
	return err
}
defer engine.Close()

if err := engine.Set("order-42", []byte("ready")); err != nil {
	return err
}
value, found, err := engine.Get("order-42")
if err != nil || !found {
	return err
}
_ = value

// Flush synchronizes the current arrangement. Compact reclaims stale bytes.
if err := engine.Flush(); err != nil {
	return err
}
```

`Path` is the arrangement file used for reopen. A later process can open the
same path with `EngineVinyl`; records survive a clean close and reopen through
the arrangement's checksummed format. `Snapshot` returns a sorted, copied view
of entries. `Stats` reports hot bytes, disk bytes, spill records, and operation
counters.

## Configuration

`MemoryLimitBytes` bounds logical key-plus-value bytes for `memtx`. For
`vinyl`, it bounds hot value-payload bytes; key and index metadata remain
resident, so this is a hot-payload budget rather than a complete process RSS
limit. `MaxDiskBytes`, `MaxKeyBytes`, and `MaxValueBytes` are enforced by the
vinyl arrangement. Zero selects the underlying default for vinyl and means no
explicit limit for the other options. Invalid engine kinds and negative limits
are rejected during `Open`.

`Flush` is the durability boundary exposed by this adapter. `Compact` is an
explicit maintenance operation. The adapter does not silently start a
background compactor, WAL replicator, cluster coordinator, or backup service;
those remain caller-owned policies.

## Measured Tradeoff

The detailed raw output is in [BENCHMARK.md](BENCHMARK.md#t-u17-selectable-vinyl-style-space-engine).
On Linux/amd64 with an AMD Ryzen 9 5950X, five benchmark samples showed that
`memtx` remains the right default for hot low-latency access. `vinyl` costs
about 1.61x on the measured Set/Get path and 7.01x on a 4,096-row lookup, with
slightly more allocation per operation. In exchange, the measured hot value
payload was 2.91x smaller and the arrangement reported 172,342 disk bytes.

This is an operational choice, not a universal optimization: use `memtx` for
ephemeral cache spaces and `vinyl` when restart persistence and bounded hot
memory justify disk I/O, compaction, and read amplification.

## Scope

The engine is an importable storage primitive. It does not add SQL semantics,
transactions, replication, encryption, or automatic multi-datacenter
partitioning. Callers that need those guarantees must layer them around the
engine and include the arrangement path in their backup and recovery policy.
