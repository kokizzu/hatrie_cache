# SQL `RETURNING` Before Rows

Hatrie Cache now exposes the pre-mutation row through `SQLMutationResult.BeforeRows` when a mutation uses `RETURNING`. `Rows` keeps its existing meaning: the post-mutation row for insert/update, and the deleted row for delete.

```go
result, err := hatCache.ExecuteSQLMutation(ctx, trie, `
UPDATE cache SET value = 'Grace' WHERE key = 'profile:1'
RETURNING key, value, exists`, nil, hatCache.SQLQueryOptions{})
if err != nil {
	return err
}
// result.BeforeRows: [{key: "profile:1", value: "Ada", exists: true}]
// result.Rows:       [{key: "profile:1", value: "Grace", exists: true}]
```

The field is populated only for an affected mutation with a `RETURNING` clause. Mutations without `RETURNING` do not capture or retain a before row, so the default write path keeps its existing allocation behavior. An insert has no before row. A conflict-update exposes the existing row as `BeforeRows`; `DO NOTHING` has no affected mutation and therefore returns no rows.

This is useful for changefeed construction and audit logging without a second read that can race with another write. The opt-in returning path adds one row-slice allocation (`+8 B/op` and `+1 alloc/op` in the benchmark fixture); it does not add cost to ordinary mutations.

## Verification

```text
make test-tr033
make test-tr033-race
make test-tr033-full
make benchmark-tr033
```

The deterministic benchmark changed from a median `7,990 ns/op`, `8,656 B/op`, `38 allocs/op` to `7,637 ns/op`, `8,664 B/op`, `39 allocs/op`. CPU variance is within noise; the measured cost is the documented `8` bytes and one allocation for the returned before-row slice.
