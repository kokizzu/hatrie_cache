# T-U22 Cross-Index Unique Constraints

`hatDataStructure.UniqueConstraintSet[T]` provides Tarantool-style alternate-key enforcement for one in-memory row store.

## Behavior

- Define one or more named `UniqueConstraint[T]` extractors.
- `Upsert` evaluates every key before changing any owner map.
- A conflict returns `UniqueConstraintConflict`, which unwraps to `ErrUniqueConstraintDuplicateKey`.
- A rejected update leaves the old row and all old keys unchanged.
- `Lookup`, `LookupByID`, `Delete`, `Clear`, `Len`, and `Contains` are safe for concurrent callers.
- The common small-constraint path uses stack scratch and stores each row once rather than duplicating the full row in every unique index.

Extractors must be deterministic and side-effect free. Keys are strings so callers can normalize email, tenant, or external identifiers before returning them. Persistence and a larger transaction coordinator remain caller responsibilities.

## Example

```go
type User struct {
	Email    string
	Username string
}

users, err := hatDataStructure.NewUniqueConstraintSet(
	[]hatDataStructure.UniqueConstraint[User]{
		{Name: "email", Extract: func(user User) string { return user.Email }},
		{Name: "username", Extract: func(user User) string { return user.Username }},
	},
	hatDataStructure.UniqueConstraintSetOptions{Capacity: 1000},
)
if err != nil {
	return err
}

if err := users.Upsert(42, User{Email: "ada@example.test", Username: "ada"}); err != nil {
	return err
}
_, err = users.Lookup("email", "ada@example.test")
```

## Benchmark

The benchmark builds 256 rows with two alternate keys. It compares two separate unique `HashIndex` instances with one `UniqueConstraintSet`; both are recreated per benchmark iteration and use `-benchmem`.

| Path | Median time | Bytes/op | Allocs/op | Relative time | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Two separate unique indexes | 42.4 us | 93,152 | 18 | 1.00x | 1.00x |
| `UniqueConstraintSet` | 30.3 us | 49,576 | 17 | **1.40x faster** | **46.8% lower** |

The result is an insertion/setup comparison, not a claim about an external database transaction. The set trades heterogeneous typed keys for string extractors and deliberately leaves durable transaction integration to the caller.
