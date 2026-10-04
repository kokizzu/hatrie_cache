# T-U46 Index Cardinality And Hot-Key Statistics

`hatDataStructure.IndexStats` is a bounded, opt-in diagnostic collector for
typed secondary indexes. It keeps only caller-supplied key hashes, not the
original keys. The collector combines HyperLogLog cardinality, exact posting
length counters, and a bounded Space-Saving hot-key table.

## Hash And Functional Indexes

`HashIndex` and `FunctionalIndex` support explicit attachment without changing
their constructors:

```go
stats := hatDataStructure.NewDefaultIndexStats()
index, err := hatDataStructure.NewHashIndex(
    func(value Row) string { return value.Region },
    hatDataStructure.HashIndexOptions{},
)
if err != nil {
    return err
}

err = index.AttachStats(stats, hashString)
if err != nil {
    return err
}

values := index.Lookup("apac")
report := index.Stats()
_ = values
_ = report

index.DetachStats()
```

`FunctionalIndex` has the same `AttachStats`, `Stats`, and `DetachStats`
methods. Attachment observes existing keys once and later accepted upserts;
lookups record hit/miss status through their exact posting length. `Lookup`,
`LookupInto`, `LookupIDs`, `LookupIDsInto`, `LookupOne`, and `Contains` are
covered where the index supports them.

The hash function is caller-owned and must be stable and well distributed:

```go
func hashString(value string) uint64 {
    hasher := fnv.New64a()
    _, _ = hasher.Write([]byte(value))
    return hasher.Sum64()
}
```

Use a project-standard non-cryptographic hash for diagnostics. The hash is a
privacy boundary, not an authentication mechanism; an attacker who knows the
hash function can still probe likely values.

## Ordered Indexes

`OrderedIndex` deliberately has no attached collector field. This keeps its
very small `Seek` and `Range` path free of a disabled observer branch. Callers
that need ordered-index diagnostics observe the operation explicitly:

```go
stats.ObserveKeyHash(hashString(start))
iterator, ok := ordered.Seek(start)
if ok {
    iterator.Close()
    stats.ObserveLookup(hashString(start), 1)
} else {
    stats.ObserveLookup(hashString(start), 0)
}
```

For a range, count the entries consumed by the iterator and pass that count as
the posting length. This is intentionally explicit because a range has two
bounds and does not have one exact posting list.

## Bounds And Tradeoffs

- The default path is unchanged for ordered indexes and does not allocate for
  any lookup in the benchmarked paths.
- Hash and functional indexes retain a two-word binding when created, with no
  per-entry statistics allocation. The collector itself is bounded by
  `HLLPrecision` and `HotKeyCapacity`.
- `HotKeyCapacity` defaults to 16 and is capped at 1024. HLL precision defaults
  to 10. Use smaller bounds for short-lived diagnostics.
- Collector updates take a mutex and run the caller's hasher, so attachment is
  for debugging/observability rather than an always-on production default.
- `Stats()` returns an owned snapshot. `DetachStats` stops future automatic
  observations but does not reset the caller-owned collector.

See [BENCHMARK.md](BENCHMARK.md#t-u46-index-cardinality-and-hot-key-statistics)
for the raw before/after samples and enabled-observer cost.
