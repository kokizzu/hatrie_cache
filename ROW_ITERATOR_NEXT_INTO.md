# Reusable SQL Row Iterator Rows

`hatSql.RowIterator[T]` keeps its existing allocating `Next` API and adds an
opt-in `NextInto` path for clients that process many rows with one destination.

```go
iterator, err := hatSql.QueryIterator[hatSql.SQLRow](ctx, conn, query, nil)
if err != nil {
	return err
}
defer iterator.Close()

row := make(hatSql.SQLRow)
for iterator.NextInto(&row) {
	consume(row)
}
return iterator.Err()
```

`NextInto` clears reusable maps, slices, pointers, and structs before decoding,
so fields omitted by a later JSON row cannot leak from an earlier row. The
caller owns the destination and must not reuse it concurrently or retain it as
an independent snapshot after the next call. `Next` remains independent-row:
callers that retain rows should continue using it.

The iterator also reuses its NDJSON envelope and releases that buffer when the
response is closed. The destination can still retain its own high-water map or
slice capacity, which is the intentional memory-versus-allocation tradeoff.

The five-sample benchmark is recorded in
[BENCHMARK.md](BENCHMARK.md#c191-reusable-sql-row-iterator-buffers).
