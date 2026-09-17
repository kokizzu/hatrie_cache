# CH-U22 Direct Columnar Append

`hatSql.TypedTable.AppendColumnar` is an explicit ClickHouse-style ingestion
primitive for appending a complete batch without converting every row through
an intermediate `Row` map. It is importable from the public `hatSql` package;
existing `Upsert` behavior and all server defaults are unchanged.

## API

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
    Name: "events",
    Columns: []hatSql.TypedTableColumn{
        {Name: "name", Kind: hatSql.TypedTableString},
        {Name: "score", Kind: hatSql.TypedTableInt64},
        {Name: "ratio", Kind: hatSql.TypedTableFloat64},
        {Name: "active", Kind: hatSql.TypedTableBool},
    },
})
if err != nil {
    return err
}

batch := hatSql.ColumnarBatch{
    Columns: map[string][]interface{}{
        "name":   {"alpha", "beta"},
        "score":  {int64(10), int64(20)},
        "ratio":  {1.5, 2.5},
        "active": {true, false},
    },
    Rows: 2,
}
_, err = table.AppendColumnar([]string{"a", "b"}, batch)
```

The batch must have one logical scalar value for every schema column and one
key per row. Plain `Columns`, dictionaries, nullable-packed columns,
bit-packed booleans, and fixed-width numeric columns are accepted through the
same logical-value contract. Callers may call `PackCompressedColumns` before
appending when their input is already suitable for the compact physical
representations.

## Correctness Contract

- The operation is append-only. Duplicate keys inside the batch and keys
  already present in the table are rejected; use `Upsert` for replacement.
- Keys are trimmed and empty keys are rejected.
- Field row counts, scalar kinds, and NULL values are validated strictly.
- Generated columns follow the existing `Upsert` rules and are evaluated once
  per row before mutation.
- All validation and generated callbacks finish before table state changes.
  A malformed field, callback error, or concurrent key conflict leaves the
  table, changefeed sequence, TTL state, patch bitmap, and storage events
  unchanged.
- Successful rows produce ordinary ordered `TypedTableChange` records,
  preserve TTL and MVCC behavior, and retain the existing storage-event
  contract.
- The method does not open paths, make network requests, evaluate code, or
  change authorization. The caller must not mutate the `ColumnarBatch` or key
  slice concurrently with the call.

## Measurement

The benchmark creates the same four-column table and appends 4,096 rows per
iteration on one CPU. The baseline converts each columnar cell to a typed row
and calls the existing `Upsert` path. Each value is a five-sample run with
`-benchmem`.

### Raw Results

```text
BenchmarkCHU22RowWiseUpsert       3068776 ns/op  5046935 B/op 12478 allocs/op
BenchmarkCHU22RowWiseUpsert       3056602 ns/op  5046935 B/op 12478 allocs/op
BenchmarkCHU22RowWiseUpsert       3037199 ns/op  5046936 B/op 12478 allocs/op
BenchmarkCHU22RowWiseUpsert       3051027 ns/op  5046936 B/op 12478 allocs/op
BenchmarkCHU22RowWiseUpsert       3564073 ns/op  5046936 B/op 12478 allocs/op
BenchmarkCHU22RowWiseUpsertPacked 3682146 ns/op  5110409 B/op 20413 allocs/op
BenchmarkCHU22RowWiseUpsertPacked 3731457 ns/op  5110409 B/op 20413 allocs/op
BenchmarkCHU22RowWiseUpsertPacked 3608562 ns/op  5110408 B/op 20413 allocs/op
BenchmarkCHU22RowWiseUpsertPacked 3534388 ns/op  5110409 B/op 20413 allocs/op
BenchmarkCHU22RowWiseUpsertPacked 4119216 ns/op  5110409 B/op 20413 allocs/op
BenchmarkCHU22AppendColumnar/plain 2117923 ns/op 2731360 B/op  8226 allocs/op
BenchmarkCHU22AppendColumnar/plain 2070632 ns/op 2731360 B/op  8226 allocs/op
BenchmarkCHU22AppendColumnar/plain 1961298 ns/op 2731360 B/op  8226 allocs/op
BenchmarkCHU22AppendColumnar/plain 1901153 ns/op 2731360 B/op  8226 allocs/op
BenchmarkCHU22AppendColumnar/plain 2064679 ns/op 2731360 B/op  8226 allocs/op
BenchmarkCHU22AppendColumnar/packed 2668311 ns/op 2794840 B/op 16161 allocs/op
BenchmarkCHU22AppendColumnar/packed 2846802 ns/op 2794840 B/op 16161 allocs/op
BenchmarkCHU22AppendColumnar/packed 2650549 ns/op 2794840 B/op 16161 allocs/op
BenchmarkCHU22AppendColumnar/packed 2527112 ns/op 2794840 B/op 16161 allocs/op
BenchmarkCHU22AppendColumnar/packed 2492606 ns/op 2794840 B/op 16161 allocs/op
```

### Median Comparison

| Input | Baseline | AppendColumnar | CPU | Heap | Allocations |
|---|---:|---:|---:|---:|---:|
| Plain columns | 3,056,602 ns/op | 2,064,679 ns/op | 1.48x faster | 1.85x lower | 1.52x fewer |
| Packed columns | 3,682,146 ns/op | 2,650,549 ns/op | 1.39x faster | 1.83x lower | 1.26x fewer |

The win comes from one batch lock, exact capacity reservation, column-wise
storage appends, and eliminating per-row `Upsert` bookkeeping. The API keeps
the explicit append-only restriction because silently changing duplicate-key
semantics would be unsafe. Very small batches may not amortize the duplicate
key map and returned change records; the default path was intentionally not
changed.

## Verification

The focused tests cover all four scalar kinds, NULLs, packed representations,
generated columns, duplicate/existing keys, malformed types, missing fields,
and atomic failure. Run the feature checks with:

```text
make format-chu22
make test-chu22
make benchmark-chu22
```
