# M247: Resume Errors For Expired Frontiers

Materialize-style historical subscriptions need to distinguish a checkpoint
that is malformed from one that was valid but has fallen behind source
retention. The existing `Resume` API has no source or retention argument, so it
continues to validate only the portable checkpoint shape and remains backward
compatible.

The additive APIs are:

```go
type QuerySubscriptionCheckpointValidator interface {
    ValidateQuerySubscriptionCheckpoint(QuerySubscriptionCheckpoint) error
}

type QuerySubscriptionCheckpointValidatorFunc func(QuerySubscriptionCheckpoint) error

subscription, err := registry.ResumeWithValidator(checkpoint, validator)
differential, err := registry.ResumeDifferentialWithValidator(checkpoint, validator)
```

The validator is called only after version, mode, query-definition, completion,
frontier-range, and revision checks pass, and before the resumed subscription
is registered. Return `ErrQuerySubscriptionCheckpointExpired` when the source
has compacted the requested frontier and the consumer must start from a fresh
snapshot. Other validator errors are wrapped with
`ErrQuerySubscriptionCheckpointInvalid`, preserving the original error for
`errors.Is`. A nil validator is rejected. The validator owns retention state;
the SQL package does not reread the query or invent a retention policy.

The original `Resume` and `ResumeDifferential` methods remain unchanged, so
existing callers pay no callback or allocation cost. The validator receives a
read-only checkpoint value and must not mutate its referenced slices or rows.

## Measurement

Five runs on Linux/amd64, AMD Ryzen 9 5950X, `go test -benchmem -count=5`:

| Path | Samples (ns/op) | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Legacy `Resume` | 1344, 1179, 1150, 1105, 1246 | 1179 | 1040 | 8 |
| `ResumeWithValidator` | 1413, 1195, 1192, 1295, 1120 | 1195 | 1040 | 8 |

The opt-in validation callback costs about 1.4% on this small resume path with
no additional heap allocation. Legacy behavior is unchanged. The feature was
kept opt-in because expiry validation requires caller-owned retention metadata.

Focused correctness, package, race, and vet checks passed, including ordinary
and differential resume, nil validators, typed expiry errors, wrapped source
errors, and structural validation ordering.
