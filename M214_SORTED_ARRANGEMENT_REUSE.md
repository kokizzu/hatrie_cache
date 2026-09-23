# M214 Sorted Arrangement Reuse

M214 adopts the Materialize-style idea of sharing one maintained arrangement
among compatible query plans. The public API is
`hatSql.NewTypedTableSortedArrangements`.

```go
registry, err := hatSql.NewTypedTableSortedArrangements(table)
if err != nil {
	return err
}

wide, err := registry.Acquire(hatSql.TypedTableSortedArrangementDefinition{
	OrderBy: []hatSql.TypedTableSortedArrangementOrder{
		{Field: "region"},
		{Field: "created_at", Descending: true},
	},
})
if err != nil {
	return err
}
defer wide.Release()

// Reuses the wide arrangement because region is an identical ORDER BY prefix.
plan, err := registry.Acquire(hatSql.TypedTableSortedArrangementDefinition{
	Field: "region",
})
if err != nil {
	return err
}
defer plan.Release()
```

## Compatibility

- Exact definitions are reused first.
- A longer existing order can serve a shorter prefix.
- Every requested field must match the existing prefix's field, direction, and
  NULL placement.
- Dictionary encoding and adaptive-dictionary flags are storage hints, so they
  do not prevent logical reuse.
- A suffix can affect the order of rows tied on the requested prefix. This is a
  valid SQL ordering refinement, but callers that require the standalone
  arrangement's row-key tie order should use
  `NewTypedTableSortedArrangement` directly.

`Apply`, `Rows`, `RowsPage`, `Checkpoint`, and `Definition` operate on shared
state. Updates should be applied once to the shared lease; all leases observe
the same checkpoint and rows. `Release` removes the arrangement after the last
lease is released. `Active` reports distinct live arrangements and `Reused`
reports whether an acquire found an existing one.

The registry is opt-in. Existing direct constructors and all default paths are
unchanged. Invalid definitions and incompatible direction/NULL combinations
still create separate arrangements or return the existing validation errors.

## Verification

Focused tests cover prefix reuse, shared updates, incompatible direction
handling, reference-counted release, and independent row snapshots. The
focused package test, race test, and vet test all pass:

```text
make m214-test
make m214-race
make m214-vet
```

## Benchmark

Five `-benchmem` samples were run on Linux/amd64 with an AMD Ryzen 9 5950X.
The direct path constructs a fresh compatible prefix arrangement for each
operation. The registry path keeps the longer arrangement live and measures a
prefix acquire, one-row read, and release.

| Path | Median ns/op | B/op | allocs/op | Relative CPU | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Direct compatible-prefix construction | 108,676 | 63,528 | 283 | 1.00x | 1.00x |
| Registry compatible-prefix reuse | 489.2 | 232 | 6 | 0.0045x (`~222x` faster) | 0.0037x (`~274x` lower) |

The registry path uses about `47x` fewer allocations. This comparison measures
the repeated-plan path, not a claim that all sorted scans become 222x faster;
the registry still retains the backing arrangement while a lease is active.

Raw samples:

```text
Direct compatible-prefix ns/op: 116000 108676 114356 107109 107462
Direct compatible-prefix B/op:   63528  63528  63528  63528  63528
Direct compatible-prefix allocs:   283    283    283    283    283
Registry reuse ns/op:             489.2  489.4  485.0  507.1  477.5
Registry reuse B/op:              232    232    232    232    232
Registry reuse allocs:              6      6      6      6      6
```

The direct composite-arrangement construction baseline in the same run was
`230,722 ns/op`, `141,950 B/op`, and `858 allocs/op`; the registry does not
duplicate that state for the compatible prefix.
