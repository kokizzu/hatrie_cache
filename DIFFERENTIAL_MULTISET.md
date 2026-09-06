# Differential Multiset

`hat/hatDataStructure` provides `DifferentialMultiset[T]` for incremental
dataflow state represented as `(data, time, diff)` records.

```go
state := hatDataStructure.NewDifferentialMultiset[string]()
_ = state.Add("key", 42, 1)
_ = state.Add("key", 42, -1) // removes the zero result
current := state.Get("key", 42)
```

Equal data and timestamps are consolidated immediately. Opposite updates
therefore cancel without retaining tombstones, and zero-diff updates never
create entries. `ForEach` visits only nonzero records; iteration order is
unspecified. The data type must be comparable because it is part of the map
key.

Diff overflow and underflow return `ErrDifferentialOverflow` without mutating
the entry. The zero value can be used and lazily initializes its map. The
multiset is not synchronized, so concurrent callers must provide their own
locking. This is an opt-in data structure and does not change existing SQL
or replication behavior.
