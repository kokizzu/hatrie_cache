# T-U16 Memtx-Style Row Table

`hatDataStructure.MemtxTable[T]` is an opt-in, fixed-capacity in-memory row
table for workloads that repeatedly scan a bounded set of typed rows. It is
inspired by Tarantool's memtx engine: rows occupy preallocated slots and a
secondary ID-to-slot map provides exact lookup.

## API

```go
type Order struct {
	CustomerID uint64
	TotalCents uint64
}

table, err := hatDataStructure.NewMemtxTable[Order](
	hatDataStructure.MemtxTableOptions{Capacity: 100_000},
)
if err != nil {
	return err
}

if err := table.Insert(42, Order{CustomerID: 7, TotalCents: 1250}); err != nil {
	return err
}
value, found := table.Get(42)
inserted, err := table.Upsert(42, Order{CustomerID: 7, TotalCents: 1500})
removed := table.Delete(42)
rows := table.ScanInto(nil)
```

`Insert` rejects duplicate IDs with `ErrMemtxTableDuplicateID`. `Upsert`
returns `inserted == true` only for a new row. A table never grows after
construction; a new insert into a full table returns `ErrMemtxTableFull`.
Capacity `0` selects `DefaultMemtxTableCapacity` (`1024`), while capacities
outside `1..MaxMemtxTableCapacity` are rejected.

`ScanInto` returns rows in physical slot order and reuses the destination
backing array when it has enough capacity. Deleting a row makes its slot
available for reuse, so physical order is not a durable ordering guarantee.
`Reset` removes all rows but retains the allocated table storage. All methods
are safe for concurrent access.

## When To Use It

Use this table when capacity is known or bounded and scans dominate the
workload. It is deliberately not the default storage for existing maps or SQL
tables. A plain map remains preferable for point-lookup-heavy workloads, and
an ordered or hash index remains preferable when ordering or secondary-key
queries are required.

The table does not provide persistence, replication, SQL integration, automatic
growth, eviction, or secondary indexes. Callers own those concerns.

## Measurements

Measured on Linux/amd64, AMD Ryzen 9 5950X, five samples, `-benchmem`, with
4096 `uint64` rows. The locked map controls include one `RWMutex` operation at
the same logical scope as the table API.

| Workload | Control median | Memtx median | Relative result | Memory |
| --- | ---: | ---: | --- | --- |
| Point lookup, unlocked map | 8.395 ns/op | 16.17 ns/op | 1.93x slower | 0 B/op, 0 allocs/op both |
| Point lookup, locked map | 10.76 ns/op | 16.17 ns/op | 1.50x slower | 0 B/op, 0 allocs/op both |
| Full scan, locked map | 33,276 ns/op | 5,126 ns/op | 6.49x faster | 0 B/op, 0 allocs/op both |
| Build 4096 rows | 88,995 ns/op | 187,348 ns/op | 2.10x slower | 147,776 -> 262,608 B/op; 17 -> 21 allocs/op |

The result is a targeted scan optimization, not a general map replacement.
The benchmark is reproducible with `make benchmark-tu16`.
