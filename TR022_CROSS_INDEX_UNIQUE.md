# T-U22 Cross-Index Unique Constraints

`hatDataStructure.UniqueIndexGroup` provides an opt-in, caller-owned primitive
for enforcing uniqueness across multiple named projections of the same row.
It is useful when one record must be unique by more than one key, such as
`email` and `phone`, and an update must not publish only half of its new index
state.

## Contract

Each `UniqueIndexConstraint[T]` contains a stable name and an extractor:

```go
group, err := hatDataStructure.NewUniqueIndexGroup(
    []hatDataStructure.UniqueIndexConstraint[User]{
        {
            Name: "email",
            Extract: func(user User) (string, bool) {
                return user.Email, user.Email != ""
            },
        },
        {
            Name: "phone",
            Extract: func(user User) (string, bool) {
                return user.Phone, user.Phone != ""
            },
        },
    },
    1024,
)
if err != nil {
    return err
}

if err := group.Upsert(userID, user); err != nil {
    var violation hatDataStructure.UniqueIndexViolation
    if errors.As(err, &violation) {
        // violation.Constraint and violation.Owner identify the conflict.
        // The conflicting key is intentionally not included in the error.
    }
}

owner, found, err := group.Lookup("email", "a@example.com")
```

`Upsert` extracts all present keys and preflights every constraint while one
write lock is held. If a key belongs to another row, it returns
`UniqueIndexViolation` and leaves the previous row and every index unchanged.
On success, old keys are removed and the complete new set is published. A row
may omit a constraint by returning `present == false`. Empty keys are allowed
when the extractor explicitly marks them present.

`Delete` is idempotent. `Lookup` and `ConstraintNames` are read-only, and
`Clear` removes all rows and index entries. The constructor rejects empty or
duplicate names, missing extractors, and more than
`MaxUniqueIndexGroupConstraints` constraints.

The error reports the constraint name and owning row, but not the raw key. This
keeps a caller from accidentally putting sensitive indexed values into logs.

## Scope and integration

The group is a generic package primitive, not an implicit index attached to
every table. A caller that owns a named space or schema can invoke `Upsert`
as part of its write transaction and persist the row only after it succeeds.
Existing single-index types and ordinary maps are unchanged, so there is no
default-path cost or behavior change.

The group is bounded to 64 constraints and uses a per-constraint string-key
map plus a row-to-key list. Existing-row updates reuse that list, so the hot
update path does not allocate. New rows still pay the normal index-entry and
row-key storage cost.

## Verification

The focused tests cover duplicate insert rejection, atomic multi-index update
rollback, key removal, idempotent deletion, constructor validation, unknown
constraints, concurrent readers/writers, and error identity. The package was
also run under the race detector and `go vet`.

Raw benchmark samples and the comparison caveat are in
`TR022_BENCHMARK_RAW.txt` and the cumulative table in `BENCHMARK.md`.
