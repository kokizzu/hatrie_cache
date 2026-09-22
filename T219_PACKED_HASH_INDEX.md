# T219: Packed HASH Index

`hat/hatDataStructure.PackedHashIndex[T, K]` is an opt-in immutable exact-match
index for read-heavy snapshots with one entry per distinct key. It fills the
gap between the existing mutable map-backed `HashIndex` and a compact
read-only table: the table uses flat open addressing, a one-bit occupancy
bitmap, and stored hash fingerprints instead of Go map buckets and lock state.

## Behavior

- `NewPackedHashIndex` copies its input entries and rejects duplicate keys.
- The table uses power-of-two linear probing with a maximum 75% load factor.
- `Lookup` and `Contains` are allocation-free and take no lock after
  construction; immutable backing arrays are safe for concurrent readers.
- `Capacity`, `Len`, and exact-key lookup are available without a snapshot
  allocation.
- There is deliberately no `Upsert` or `Delete`; rebuild a new table when the
  snapshot changes.
- The caller supplies a deterministic `func(K) uint64`, so callers can select
  a domain-specific fast hash. The function must remain stable for the table's
  lifetime and should distribute keys uniformly.

```go
entries := []hatDataStructure.HashIndexEntry[int, string]{
    {ID: 1, Key: "apac", Value: 10},
    {ID: 2, Key: "eu", Value: 20},
}
index, err := hatDataStructure.NewPackedHashIndex[int, string](hashString, entries)
entry, ok := index.Lookup("apac")
```

The existing `HashIndex` remains the right choice for mutable data, updates,
deletes, non-unique postings, and reverse-ID maintenance. This feature does
not automatically replace it, and it does not change SQL, persistence,
replication, backup, or wire behavior.

## Measurement

Command: `make benchmark-t219` on an AMD Ryzen 9 5950X, Go benchmark mode,
three samples, 65,536 integer keys. The comparison is the existing mutable
map-backed `HashIndex` with the same exact keys. Build `B/op` measures allocated
heap during construction; it is not a claim about the runtime's exact retained
map bucket size.

| Workload | Packed hash | Mutable `HashIndex` | Relative result |
| --- | ---: | ---: | --- |
| Exact lookup | 8.851 ns/op; 0 B/op; 0 allocs/op | 50.15 ns/op; 0 B/op; 0 allocs/op | 5.7x faster; allocation-neutral |
| Build 65,536 keys | 1,136,872 ns/op; 4,210,752 B/op; 3 allocs/op | 7,222,396 ns/op; 5,859,754 B/op; 517 allocs/op | 6.3x faster; 1.4x lower heap; 172x fewer allocation events |

Raw samples:

```text
BenchmarkT219PackedHashIndexLookup/packed-open-addressing-32  28921321  8.851 ns/op        0 B/op   0 allocs/op
BenchmarkT219PackedHashIndexLookup/packed-open-addressing-32  26833866  8.457 ns/op        0 B/op   0 allocs/op
BenchmarkT219PackedHashIndexLookup/packed-open-addressing-32  33055125  8.952 ns/op        0 B/op   0 allocs/op
BenchmarkT219PackedHashIndexLookup/mutable-map-32              4800181 50.15  ns/op        0 B/op   0 allocs/op
BenchmarkT219PackedHashIndexLookup/mutable-map-32              4319148 48.24  ns/op        0 B/op   0 allocs/op
BenchmarkT219PackedHashIndexLookup/mutable-map-32              4834147 50.30  ns/op        0 B/op   0 allocs/op
BenchmarkT219PackedHashIndexBuild/packed-open-addressing-32         183  1100350 ns/op  4210752 B/op   3 allocs/op
BenchmarkT219PackedHashIndexBuild/packed-open-addressing-32         214  1136872 ns/op  4210752 B/op   3 allocs/op
BenchmarkT219PackedHashIndexBuild/packed-open-addressing-32         201  1238504 ns/op  4210752 B/op   3 allocs/op
BenchmarkT219PackedHashIndexBuild/mutable-map-32                     32  6286280 ns/op  5859754 B/op 517 allocs/op
BenchmarkT219PackedHashIndexBuild/mutable-map-32                     42  8824430 ns/op  5859768 B/op 517 allocs/op
BenchmarkT219PackedHashIndexBuild/mutable-map-32                     45  7222396 ns/op  5859753 B/op 517 allocs/op
```

The tradeoff is intentional: this is a fast, compact read snapshot, not a
general mutable secondary index. Rebuilding is required after changes, and a
poor caller-supplied hash can increase probe length. Keep it opt-in until the
workload is demonstrably snapshot-oriented.
