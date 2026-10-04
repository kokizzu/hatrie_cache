# T-U22 Cross-Index Unique Constraints

T-U22 adds `hatDataStructure.UniqueConstraintSet`, an opt-in sidecar for rows
that need several unique projections to be admitted or rejected atomically.
The caller supplies one string key per named constraint in a fixed order:

```go
constraints, err := hatDataStructure.NewUniqueConstraintSet(
	hatDataStructure.UniqueConstraintSetOptions{
		ConstraintNames: []string{"email", "external_code"},
		Capacity:        100_000,
		MaxKeyBytes:     1 << 20,
	 },
)
if err != nil {
	 panic(err)
}

err = constraints.Upsert(rowID, []string{email, externalCode})
```

`Upsert` checks every namespace under one lock before changing any reservation.
If any key belongs to another row, it returns an
`*UniqueConstraintConflictError` and leaves every namespace unchanged.
Successful replacement removes the old keys, and `Delete` releases all keys.
`Owner`, `OwnerAt`, `Len`, and `ConstraintNames` provide bounded inspection.

The set is deliberately separate from a query index: the caller can maintain
normal hash, ordered, functional, or text indexes alongside it. It stores only
unique key ownership, uses one flat row-key arena, and reuses deleted row slots.
Keys are bounded to 1 MiB by default; configure a lower limit when inputs are
untrusted. Constraint names are copied and must be non-empty and unique. The
feature is opt-in and does not change existing index or SQL defaults.

## Measurement

The baseline is two independent unique `HashIndex` instances with a caller
rollback after the second index rejects a write. The candidate is one
`UniqueConstraintSet` with the same two string projections. Five `-count=5`
samples were run on Linux/amd64, AMD Ryzen 9 5950X, with 10,000 prepared rows.

| Workload | Baseline median | Candidate median | Improvement |
| --- | ---: | ---: | ---: |
| Steady upsert | 142.5 ns/op, 0 B/op, 0 allocs/op | 64.8 ns/op, 0 B/op, 0 allocs/op | 2.20x faster |
| Build 10,000 rows | 2.595 ms/op, 2,972,524 B/op, 139 allocs/op | 1.379 ms/op, 1,497,297 B/op, 106 allocs/op | 1.88x faster, 1.98x lower bytes, 1.31x fewer allocs |

Run the permanent benchmark target with `make benchmark-tu22-cross-index-unique`.
Raw samples are recorded in `BENCHMARK.md`. The comparison is appropriate for
the constraint-maintenance portion only: the candidate does not replace a
caller-owned index that stores full row values or postings.

Verification includes focused tests, race detection, `go vet`, and the full
`hat/hatDataStructure/...` package test set. Existing SQL and storage defaults
remain unchanged.
