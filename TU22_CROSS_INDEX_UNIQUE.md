# T-U22 Cross-Index Unique Constraints

`hatDataStructure.CrossIndexUniqueSet[T]` provides an opt-in, concurrency-safe
owner for several unique projections over the same row IDs. It is intended for
Tarantool-style spaces with independent unique indexes such as `email`,
`external_id`, or a derived functional key.

```go
type User struct {
	Email string
	Code  string
}

set, err := hatDataStructure.NewCrossIndexUniqueSet([]hatDataStructure.CrossIndexUniqueConstraint[User]{
	{
		Name: "email",
		Key: func(user User) (string, bool, error) {
			return user.Email, user.Email != "", nil
		},
	},
	{
		Name: "code",
		Key: func(user User) (string, bool, error) {
			return user.Code, user.Code != "", nil
		},
	},
}, 1024)
if err != nil {
		// Invalid definitions fail before a set is published.
		return err
}

if err := set.ApplyBatch([]hatDataStructure.CrossIndexUniqueMutation[User]{
	{ID: 1, Value: User{Email: "ada@example.test", Code: "A1"}},
	{ID: 2, Value: User{Email: "grace@example.test", Code: "G1"}},
}); err != nil {
	return err
}
```

## Semantics

- `Upsert` evaluates every projection before changing any row or owner map.
- `ApplyBatch` validates the entire batch first. Duplicate IDs are rejected;
  a conflict in any projection leaves the complete prior state unchanged.
- A constraint can return `present=false` for SQL NULL-like values. Multiple
  absent values do not conflict; an empty string is still a present key when
  the extractor says it is present.
- Replacing one ID removes all old projection keys and installs all new keys
  as one operation. `LookupID` returns the owning row ID for a named key.
- Conflict errors expose the constraint and owner/candidate IDs, but never the
  key value itself.
- The set is in-memory and deliberately does not alter existing cache, SQL,
  journal, backup, or replication defaults. A caller that needs durability must
  rebuild the set from its durable row source after restore.

The constructor bounds the number of constraints to 64, constraint names to
128 bytes, retained keys to 1 MiB each, and one atomic batch to 4096 mutations.
Extractor failures and invalid bounds fail before state mutation.

## Measurement

The benchmark used two unique string projections and repeated replacement of
1,024 existing IDs on Linux/amd64, AMD Ryzen 9 5950X. Both paths used one
uncontended mutex and reported no allocations after setup. Five raw samples
were collected; the table reports the median.

| Path | ns/op | B/op | allocs/op | Relative CPU |
|---|---:|---:|---:|---:|
| Hand-written two-map baseline | 139.0 | 0 | 0 | 1.00x |
| `CrossIndexUniqueSet.Upsert` | 286.9 | 0 | 0 | 2.06x slower |

The set is therefore a correctness and maintenance feature, not a speed
optimization. Its cost is opt-in and buys atomic validation across all
projections, deterministic conflict reporting, and one concurrency boundary.
No existing write or query path was changed to use it automatically.

Raw `ns/op` samples were `129.2, 139.0, 131.3, 142.0, 139.0` for the
baseline and `289.4, 290.9, 282.8, 276.9, 286.9` for the set. Both paths
reported `0 B/op` and `0 allocs/op` in every sample.

Focused correctness, public-API, race, vet, and package tests pass. The
package-wide test command was also run after implementation.
