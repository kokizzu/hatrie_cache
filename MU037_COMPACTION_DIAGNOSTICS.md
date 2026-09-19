# M-U37 Per-Arrangement Compaction Diagnostics

This adds an opt-in bounded registry for Materialize-style per-arrangement
compaction telemetry. It records logical bytes, physical bytes, caller-defined
compaction debt, input/output bytes, duration, outcome, and a bounded recent
history for each arrangement.

```go
diagnostics, err := hatStorage.NewCompactionDiagnostics(hatStorage.CompactionDiagnosticsOptions{
	MaxArrangements:       256,
	HistoryPerArrangement: 8,
})
if err != nil {
	return err
}
if err := diagnostics.Register("orders_by_customer"); err != nil {
	return err
}
if err := diagnostics.Record(hatStorage.CompactionObservation{
	Arrangement:         "orders_by_customer",
	LogicalBytes:        64 << 20,
	PhysicalBytes:       48 << 20,
	CompactionDebtBytes: 8 << 20,
	InputBytes:          72 << 20,
	OutputBytes:         48 << 20,
	Duration:            12 * time.Millisecond,
	Outcome:             hatStorage.CompactionSucceeded,
}); err != nil {
	return err
}
snapshot := diagnostics.Snapshot()
```

## Contract

- Registration is explicit and bounded. Unknown names are rejected instead of
  silently growing the registry or evicting telemetry.
- `Snapshot` is sorted by arrangement name and returns detached values. Each
  arrangement keeps only the configured oldest-to-newest ring history.
- `CompactionDebtBytes` is supplied by the storage engine. HAT-trie, LSM, and
  remote-part engines have different definitions of obsolete or queued bytes,
  so the registry does not invent a misleading estimate.
- Arrangement labels are length- and UTF-8-validated. The registry stores no
  keys, values, file paths, or query text.
- The zero-value options select 256 arrangements, 8 samples per arrangement,
  and 128-byte labels. The registry itself is opt-in; existing storage and
  compaction paths do not allocate or record anything unless a caller creates
  and wires one.
- `Record` is allocation-free after registration. `Snapshot` intentionally
  allocates detached copies and should be called by a scrape or operator
  endpoint rather than the compaction hot loop.

## Measured Cost

AMD Ryzen 9 5950X, five benchmark samples, `go test -benchmem`:

| Operation | Median | Memory | Comparison |
|---|---:|---:|---:|
| Plain local counters | 1.442 ns/op | 0 B/op, 0 allocs/op | Baseline |
| Registered `Record` | 37.58 ns/op | 0 B/op, 0 allocs/op | 26.06x slower, 0 allocations |
| 64-arrangement `Snapshot` | 11,195 ns/op | 14,720 B/op, 66 allocs/op | Explicit scrape cost |

The feature is therefore a bounded observability capability, not a faster
replacement for raw counters. Its write cost is paid only by callers that
enable it, and its default path has no registry or lock overhead.

## Verification

```text
make test-mu37
make benchmark-mu37-baseline
make benchmark-mu37
make verify-mu37
```

The focused tests cover validation, capacity bounds, deterministic ordering,
ring truncation, detached snapshots, unregistering, and concurrent recording.
