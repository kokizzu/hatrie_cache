# Index Cardinality And Hot-Key Statistics

`hatDataStructure.IndexStats` is an opt-in, fixed-memory diagnostic collector
for secondary-index planning. It combines the existing HyperLogLog estimator
with exact posting-length totals and a bounded Space-Saving heavy-hitter table.
Index implementations are not instrumented automatically; callers choose
where to observe keys and lookups.

## Usage

```go
stats := hatDataStructure.NewDefaultIndexStats()
keyHash := uint64(42)
postingLength := uint64(3)

// Observe keys while building or refreshing an index.
stats.ObserveKeyHash(keyHash)

// Observe a lookup without retaining the original key or posting list.
stats.ObserveLookup(keyHash, postingLength)

report := stats.Snapshot()
fmt.Println(report.EstimatedDistinctKeys, report.MaxPostingLength)
```

Pass a stable, well-distributed `uint64` hash rather than the raw key. This
keeps diagnostic memory bounded and avoids retaining sensitive key material.
The same hash must be used consistently by the caller; hash collisions can
merge diagnostic entries and make hot-key results advisory.

## Report Fields

| Field | Meaning |
| --- | --- |
| `CardinalityObservations` | Number of explicit `ObserveKeyHash` calls. |
| `EstimatedDistinctKeys` | Approximate distinct hashes seen through either observation method. |
| `LookupObservations` | Number of observed lookups. |
| `PostingLengthSamples` | Number of posting lengths supplied to `ObserveLookup`. |
| `TotalPostingLength` | Exact sum of supplied posting lengths. |
| `MaxPostingLength` | Exact maximum supplied posting length. |
| `HotKeys` | At most `HotKeyCapacity` approximate heavy hitters, sorted by frequency. |

Each hot-key entry reports its hash, estimated observation count, Space-Saving
error bound, and maximum posting length seen for that entry. `HotKeys` is copied
and sorted in every snapshot; mutating a returned snapshot does not mutate the
collector.

## Cardinality And Lifecycle

HyperLogLog is cumulative and cannot remove a key. For current index
cardinality, observe the complete index during a rebuild or start a new
collector after a reset boundary. `ObserveLookup` also contributes its hash to
the estimate, so lookup-only instrumentation estimates the working-set
cardinality rather than necessarily the full stored index cardinality.

The collector is safe for concurrent observations and snapshots. Observation
methods use a mutex because the HyperLogLog registers and fixed hot-key table
are mutable; reporting allocates only the returned hot-key slice.

Defaults are deliberately bounded:

- HyperLogLog precision is 10, using 1,024 registers.
- The hot-key table holds 16 entries by default.
- Hot-key capacity is limited to 1,024 entries.
- A zero option field selects its default.

Use a lower-level or external metrics system for raw key values, exact
postings, or unbounded per-key histories.

## Measurement

Five samples with `-benchtime=250ms` on Linux/amd64 and an AMD Ryzen 9 5950X
gave these medians from `make benchmark-t-u46`:

| Operation | Median | Memory |
| --- | ---: | ---: |
| `ObserveKeyHash` | 15.67 ns/op | 0 B/op, 0 allocs/op |
| `ObserveLookup` with 16-slot hot-key table | 33.22 ns/op | 0 B/op, 0 allocs/op |

These numbers are the cost when the collector is enabled; existing indexes pay
nothing until their callers invoke an observation method. The tradeoff is
mutex contention under very high concurrent observation rates and approximate,
hash-only hot-key identity. Keep the collector off the hot path when those
diagnostics are not needed, or use separate collectors for independent index
workloads.

## Verification

Focused tests cover exact posting counters, bounded heavy-hitter replacement,
snapshot ownership, invalid bounds, and concurrent observation:

```text
make test-t-u46
make benchmark-t-u46
```

The publisher additionally runs the race detector, `go vet`, and the complete
repository test suite.
