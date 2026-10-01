# T-U16 Selectable Memtx-Style Row Engine

`hatDataStructure.MemtxRowTable` is an opt-in fixed-width in-memory tuple
engine inspired by Tarantool's memtx space. It is useful for small or
scan-heavy spaces where a predictable row-major layout is preferable to one
slice allocation per map entry.

```go
table, err := hatDataStructure.NewMemtxRowTableWithCapacity(4, 10_000)
if err != nil {
	return err
}
_ = table.Upsert("user-1", []any{int64(1), "SG", "Ada", true})

values, found := table.GetInto(make([]any, 4), "user-1")
if found {
	// values is caller-owned and can be reused for the next lookup.
	_ = values
}

table.VisitBorrowed(func(key string, values []any) bool {
	// The values slice is valid only during this callback.
	return true
})
```

The table uses one key-to-slot map and a shared row-major value array. `Get`
and `Visit` return detached values; `GetInto` and `VisitBorrowed` are the
allocation-free hot paths. `Delete` clears value references immediately but
leaves physical slots until the caller invokes `Compact`. The zero value is a
valid key-only table. The table is safe for concurrent readers and writers.

This is not the default storage engine and is not wired into existing SQL
tables automatically. Existing HAT-trie and typed-table behavior is unchanged.
Use it when the workload is scan-heavy or when predictable tuple allocation
matters more than the lowest possible single-key lookup latency.

## Measurement

The five-sample benchmark used 10,000 rows with four values per row and
compared the new table with a `map[string][]any` baseline. The baseline is a
controlled layout comparison, not a claim that every existing table uses that
exact representation.

| Operation | Map baseline | Memtx row table | Result |
| --- | ---: | ---: | --- |
| Build | 1,261,892 ns/op; 1,505,036 B/op; 19,777 allocs | 1,084,704 ns/op; 1,336,245 B/op; 9,782 allocs | 1.16x faster; 11.2% lower bytes; 2.02x fewer allocs |
| Point lookup | 18.08 ns/op; 0 B/op; 0 allocs | 29.83 ns/op; 0 B/op; 0 allocs | 1.65x slower |
| Full scan | 147,202 ns/op; 0 B/op; 0 allocs | 32,839 ns/op; 0 B/op; 0 allocs | 4.48x faster |
| Full-row update | 28.28 ns/op; 0 B/op; 0 allocs | 34.96 ns/op; 0 B/op; 0 allocs | 1.24x slower |

The result supports a selectable engine rather than a default replacement:
scan-heavy workloads get the large win, while point-heavy workloads should
keep the existing map-oriented path. All valid-path scan and reusable lookup
operations remain allocation-free.

## Verification

```text
make test-t-u16-memtx-row-table
make benchmark-t-u16-memtx-row-table
make verify-t-u16-memtx-row-table
```

Focused tests cover fixed-width validation, zero-value use, detached reads,
borrowed scans, updates, deletes, and compaction. Full package tests, race,
and vet passed.
