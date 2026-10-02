# T-U22 Cross-Index Unique Constraints

`hatDataStructure.CrossIndexUnique[T]` atomically maintains several named
unique constraints for one value type. It fills the gap between independent
unique `HashIndex` instances and a space-level write contract: a row can be
required to satisfy both a single-field key and a composite key without a
failed second check leaving the first index changed.

## Example

```go
type User struct {
	Email    string
	Tenant   string
	Username string
}

constraints := []hatDataStructure.CrossIndexUniqueConstraint[User]{
	{
		Name: "email",
		Key: func(user User) (string, error) { return strings.ToLower(user.Email), nil },
	},
	{
		Name: "tenant_username",
		Key: func(user User) (string, error) {
			return user.Tenant + "\x00" + user.Username, nil
		},
	},
}
registry, err := hatDataStructure.NewCrossIndexUnique(constraints, 1024)
if err != nil {
		return err
}
if err := registry.Upsert(userID, user); err != nil {
		return err
}
```

`Upsert` derives and validates every key before it changes any map. A conflict
returns `ErrCrossIndexUniqueDuplicate` and leaves the old value and every
constraint untouched. Updating an existing ID removes old keys and installs
new keys under the same lock. `Delete`, `Lookup`, `Contains`, `Clear`, and
`ConstraintNames` provide the corresponding lifecycle and inspection APIs.

## Contract and limits

- Constraint names are immutable, non-empty, and unique.
- Key functions must return deterministic canonical strings and should be
  non-blocking because they run under the registry write lock.
- Empty keys are valid; callers that treat empty values as NULL must encode
  that policy explicitly in the key function.
- The registry is in-memory and process-local. The caller still owns row
  storage, WAL/journal ordering, replication, and recovery.
- The coordinator protects its own indexes with one lock; it does not make a
  separate storage write transaction atomic with the index update.
- Upserts with unchanged keys use an inline scratch path and avoid the
  coordinator's key-slice allocation. Changed-key updates retain the exact
  old keys until all new keys pass validation.

## Verification

Tests cover first insert, single-field and composite conflicts, atomic
rollback for a conflict on a later constraint, changed-key cleanup, key
extraction errors, invalid definitions, clear/lookup behavior, more than the
inline constraint count, and concurrent upserts/lookups:

```sh
make round28-focused
make round28-race-focused
make round28-test
make round28-vet
```

The feature is opt-in. Existing `HashIndex`, `FunctionalIndex`, and
`OrderedIndex` behavior is unchanged.

## Benchmark interpretation

The benchmark compares a two-map manual implementation with the coordinator
for repeated updates of 64 resident values. Same-key updates exercise the
common no-reindex fast path. Changed-key updates exercise full removal and
re-insertion. The changed-key cost is intentional: the coordinator provides
multi-index atomicity and concurrency safety that a raw map sequence does not.

Raw samples from the same `-benchtime=1s -count=5 -benchmem` runs:

| Workload | Manual baseline samples (ns/op) | Coordinator samples (ns/op) |
|---|---|---|
| Same-key update | 145.4, 147.7, 138.8, 140.0, 147.6 | 114.1, 113.8, 115.1, 115.9, 115.9 |
| Changed-key update | 151.4, 152.3, 151.4, 162.0, 159.8 | 206.0, 205.7, 203.6, 201.5, 209.1 |
