# Generated Columns

`TypedTableColumn.Generated` adds an opt-in generated column to a
`hatSql.TypedTable`. The callback receives a schema-ordered copy of the
candidate row and returns the typed value stored for that column.

```go
orders, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
	Name: "orders",
	Columns: []hatSql.TypedTableColumn{
		{Name: "price", Kind: hatSql.TypedTableInt64},
		{Name: "quantity", Kind: hatSql.TypedTableInt64},
		{
			Name: "total",
			Kind: hatSql.TypedTableInt64,
			Generated: func(row []hatSql.TypedTableValue) (hatSql.TypedTableValue, error) {
				return hatSql.TypedInt64(row[0].Int64 * row[1].Int64), nil
			},
		},
	},
})
```

Generated columns are evaluated in declaration order. Earlier generated
values are available to later callbacks. The input row must still contain one
value per schema column for compatibility with `Upsert`; values supplied for a
generated position are ignored. A callback may return `TypedNull()` when the
derived value is SQL NULL, or an error to reject the whole mutation.

The returned value must match the declared `TypedTableKind`. Derived values
are included in `Rows`, columnar SQL scans, changefeeds, MVCC snapshots, and
restore/replay data. Callbacks should be deterministic, side-effect free, and
should treat their input as read-only. Existing tables without callbacks keep
the original path and allocation behavior.
