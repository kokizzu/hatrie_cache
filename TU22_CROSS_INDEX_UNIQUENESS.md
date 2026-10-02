# T-U22 Cross-Index Uniqueness

`hatDataStructure.CrossIndexUniqueSet[T]` is an opt-in coordinator for records
that must be unique under more than one derived key. It is useful for a user
record that must have both a unique email and a unique username, or for a
space that has several alternate identities.

## Contract

Create the set with one or more named key definitions:

```go
type User struct {
	ID       uint64
	Email    string
	Username string
	Team     string
}

users, err := hatDataStructure.NewCrossIndexUniqueSet(
	[]hatDataStructure.CrossIndexUniqueDefinition[User]{
		{Name: "email", Key: func(user User) string { return user.Email }},
		{Name: "username", Key: func(user User) string { return user.Username }},
	},
	hatDataStructure.CrossIndexUniqueOptions{},
)
if err != nil {
	return err
}
```

`Upsert(id, value)` derives every key before changing state. It rejects a
conflict with `ErrCrossIndexUniqueConflict`, and the failed operation leaves
all index owners unchanged. Replacing an existing ID removes its old keys and
installs its new keys as one operation. `Delete` removes every key for the
record and reports whether the ID existed.

`LookupOwner(indexName, key)` returns the ID that owns a key. `IndexNames`
returns the configured names in definition order, and `Len` reports admitted
records. The set intentionally returns owners rather than exposing a second
record store; callers can keep their authoritative rows in the space or
transaction layer.

## Bounds and defaults

- `MaxEntries: 0` selects a bounded default of `1,048,576` records.
- `Capacity` is an optional initial map-capacity hint and defaults to zero.
- Capacity cannot exceed `MaxEntries`.
- At most 64 indexes and 16,777,216 entries are accepted by the constructor.
- Index names are trimmed, must be valid UTF-8, and are limited to 128 bytes.
- Empty derived keys are legal and therefore unique like any other key.
- The feature is opt-in; it changes no existing `HashIndex` or space behavior.

The default entry limit is a memory guard, not a promise that a process can
hold that many records. Size the limit and capacity for the deployment's
record and key lengths.

## Atomicity and concurrency

The coordinator owns one lock for all maintained indexes. Conflict checks and
the replacement of every index occur under that lock, so readers cannot see a
partial multi-index update. Key functions run before the lock and should be
pure, bounded, and independent of mutable set state. The constructor copies
the definitions, and `IndexNames` returns a copy.

The existing-key fast path keeps index maps untouched when only non-indexed
fields change. Both stable-key updates and changing-key updates are
allocation-free after construction in the measured workload.

## Errors

- `ErrCrossIndexUniqueNil` means a method was called on a nil set.
- `ErrCrossIndexUniqueInvalid` means definitions or options are invalid.
- `ErrCrossIndexUniqueConflict` means another ID owns at least one derived key.
- `ErrCrossIndexUniqueLimit` means the configured entry or index bound would be
  exceeded.

## Verification

```sh
make test-tu22
make test-tu22-package
make race-tu22
make vet-tu22
make benchmark-tu22-before
make benchmark-tu22-comparison
make verify-tu22
```

The baseline benchmark is a sequential two-map implementation. It is useful
for separating map-work cost from the atomic coordinator contract; it is not a
replacement when concurrent writers must observe all unique constraints as
one operation.

## Raw comparison samples

Initial coordinator before the unchanged-key fast path, five samples:

```text
213.9 202.1 204.4 218.8 205.7 ns/op; 0 B/op; 0 allocs/op
```

Final comparison, unchanged keys:

```text
91.83 87.68 83.33 93.77 90.36 ns/op; 0 B/op; 0 allocs/op
124.7 124.0 142.4 136.3 140.0 ns/op; 0 B/op; 0 allocs/op
```

Final comparison, changing keys:

```text
95.35 99.68 87.90 98.98 102.2 ns/op; 0 B/op; 0 allocs/op
131.5 135.0 121.5 140.3 134.2 ns/op; 0 B/op; 0 allocs/op
```

The first line in each final pair is `CrossIndexUniqueSet`; the second is the
sequential two-map baseline. The benchmark is CPU and allocation focused; the
configured entry bound and per-entry key-slice ownership are the memory
controls for this primitive.
