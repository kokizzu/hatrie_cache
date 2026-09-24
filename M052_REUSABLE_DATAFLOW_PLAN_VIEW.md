# Reusable SQL Dataflow Plan View

`CompiledSQLQuery.DataflowPlanView()` exposes a read-only view over the
memoized SQL dataflow plan. It is intended for repeated metadata inspection,
explain output, observability, and tooling that does not need to mutate the
plan.

```go
query, err := hatSql.CompileSQLQuery(
	"FROM CACHE('items') SELECT id WHERE id >= 2 ORDER BY id LIMIT 4",
)
if err != nil {
	return err
}

view := query.DataflowPlanView()
for index := 0; index < view.FragmentCount(); index++ {
	fragment, ok := view.Fragment(index)
	if !ok {
		continue
	}
	fmt.Printf("%d: %s (%d inputs)\n", fragment.ID(), fragment.Kind(), fragment.InputCount())
}
```

The first view request initializes the query's memoized plan. Reusing the view
for metadata-only work does not clone the fragment or input slices and does not
allocate. The view is invalid for a nil query or its zero value, and invalid
fragment indexes return `ok == false`.

`LowerDataflow()` remains available when a caller needs an independent,
mutable plan copy. The new view does not change query execution or the existing
lowering contract.

## API

- `CompiledSQLQuery.DataflowPlanView()` returns the read-only plan view.
- `SQLDataflowPlanView.Valid()`, `Format()`, `Source()`, `Root()`, and
  `FragmentCount()` expose plan metadata.
- `SQLDataflowPlanView.Fragment(index)` returns a fragment view and a validity
  flag.
- `SQLDataflowFragmentView.Valid()`, `ID()`, `Kind()`, `Detail()`,
  `InputCount()`, and `Input(index)` expose fragment metadata without exposing
  mutable backing slices.

## Measured Cost

The benchmark inspects the same compiled query metadata in a loop:

| Path | Time | Heap | Allocations |
| --- | ---: | ---: | ---: |
| `DataflowPlanView` | 14.72-16.20 ns/op | 0 B/op | 0 allocs/op |
| `LowerDataflow` then inspect | 251.3-271.6 ns/op | 352 B/op | 5 allocs/op |

The benchmark compiles and initializes the query outside the timed loop. The
view therefore measures repeated read-only inspection, while `LowerDataflow`
includes the independent-copy cost required by its mutable contract.
