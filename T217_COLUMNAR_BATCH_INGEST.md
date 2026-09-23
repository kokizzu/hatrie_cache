# T217 Columnar Batch Ingest

T217 adds `hatSql.TypedTable.AppendColumnarBatch` for analytical ingest. The
existing `TypedTable` already stores scalar fields in per-column primitive
slices; this API makes the write path columnar too instead of calling
`Upsert` once per row.

## API

```go
batch := hatSql.TypedTableColumnarBatch{
	Keys: []string{"event-1", "event-2"},
	Columns: [][]hatSql.TypedTableValue{
		{hatSql.TypedString("apac"), hatSql.TypedString("emea")},
		{hatSql.TypedInt64(11), hatSql.TypedInt64(22)},
		{hatSql.TypedBool(true), hatSql.TypedBool(false)},
	},
}

inserted, err := table.AppendColumnarBatch(batch)
```

`Columns` is schema ordered and every column must have exactly `len(Keys)`
values. Values are copied into the table before return, so the caller may
reuse its input buffers afterward.

## Semantics

- The complete batch is validated before mutation.
- Keys are trimmed using the same rule as `Upsert`.
- Empty keys, duplicate keys, wrong column counts, wrong column lengths, and
  wrong scalar kinds return `ErrTypedTableColumnarBatchInvalid`.
- A key already present in the table returns
  `ErrTypedTableColumnarBatchKeyExists`; use `Upsert` for replacement or update
  semantics.
- Memory-budget admission is checked for the whole batch. A rejected batch
  leaves row count, columns, changefeed sequence, and memory accounting
  unchanged.
- Generated columns are materialized with the same callbacks as `Upsert`.
- TTL deadlines, dictionary storage, patch-part bitmaps, MVCC records,
  storage events, SQL columnar caches, and one ordered `INSERT` change per row
  retain their existing behavior.

The operation takes one table write lock and invalidates derived columnar
layouts once per batch. It is therefore intended for new analytical rows,
while point updates and replacements should continue using `Upsert`.

## Measured Tradeoff

The benchmark inserts 2,048 rows with string, int64, and bool columns. Table
construction is included equally in both paths. Each final result uses five
`-benchmem` samples on Linux/amd64 with an AMD Ryzen 9 5950X.

| Workload | Median ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: |
| Row-wise `Upsert` control | 1,495,132 | 1,806,735 | 4,228 | 1.00x |
| `AppendColumnarBatch` | 1,170,323 | 1,553,082 | 2,177 | 1.28x faster |

The batch path is about 14% lower in cumulative allocated bytes and 1.94x
lower in allocation count. It does not remove the per-row changefeed records,
so workloads that disable or avoid changefeeds may show a larger gain. The
API intentionally rejects existing keys rather than silently changing
`Upsert` semantics.

Run the focused checks and benchmark with:

```text
make test-t217
make race-t217
make vet-t217
make benchmark-t217
```

## Raw Results

The standalone control measurement is retained for comparison, followed by
the paired final run that measures both paths in the same process invocation.

```text
Before, make benchmark-t217-before:
BenchmarkT217TypedTableRowUpsertBaseline-32    164  1850242 ns/op  1806813 B/op  4228 allocs/op
BenchmarkT217TypedTableRowUpsertBaseline-32    100  2034172 ns/op  1806758 B/op  4228 allocs/op
BenchmarkT217TypedTableRowUpsertBaseline-32    152  1508562 ns/op  1806735 B/op  4228 allocs/op
BenchmarkT217TypedTableRowUpsertBaseline-32    100  2077442 ns/op  1806750 B/op  4228 allocs/op
BenchmarkT217TypedTableRowUpsertBaseline-32    146  1586335 ns/op  1806737 B/op  4228 allocs/op

After, make benchmark-t217:
BenchmarkT217TypedTableRowUpsertBaseline-32      148  1495132 ns/op  1806745 B/op  4228 allocs/op
BenchmarkT217TypedTableRowUpsertBaseline-32      166  1517405 ns/op  1806733 B/op  4228 allocs/op
BenchmarkT217TypedTableRowUpsertBaseline-32      157  1711138 ns/op  1806738 B/op  4228 allocs/op
BenchmarkT217TypedTableRowUpsertBaseline-32      156  1484794 ns/op  1806735 B/op  4228 allocs/op
BenchmarkT217TypedTableRowUpsertBaseline-32      176  1478460 ns/op  1806733 B/op  4228 allocs/op
BenchmarkT217TypedTableAppendColumnarBatch-32    198  1209917 ns/op  1553082 B/op  2177 allocs/op
BenchmarkT217TypedTableAppendColumnarBatch-32    195  1170323 ns/op  1553083 B/op  2177 allocs/op
BenchmarkT217TypedTableAppendColumnarBatch-32    205  1148628 ns/op  1553083 B/op  2177 allocs/op
BenchmarkT217TypedTableAppendColumnarBatch-32    200  1213057 ns/op  1553081 B/op  2177 allocs/op
BenchmarkT217TypedTableAppendColumnarBatch-32    200  1159740 ns/op  1553081 B/op  2177 allocs/op
```
