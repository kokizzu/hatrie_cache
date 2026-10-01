# T-U05 Session Transaction Settings

T-U05 adds an importable, allocation-light contract for session transaction
defaults and per-transaction overrides. It is inspired by Tarantool-style
session transaction configuration, but remains additive: the existing SQL
executor and WAL paths are unchanged unless a caller explicitly uses the new
API.

## API

```go
session, err := hatSql.NewSQLTransactionSettingsSession(
    hatSql.DefaultSQLTransactionSettings(),
)
if err != nil {
    return err
}

scope, err := session.Begin(ctx, hatSql.SQLTransactionSettingsPatch{
    ReadOnly:    true,
    ReadOnlySet: true,
    Timeout:     2 * time.Second,
    TimeoutSet:  true,
})
if err != nil {
    return err
}
defer scope.Rollback()

// The caller's executor consumes the captured policy and context.
result, err := execute(ctx, scope.Settings(), scope.Context())
if err != nil {
    return err
}
return scope.Commit()
```

The safe normalized defaults are `READ COMMITTED`, writable, no additional
deadline, and durable. Session reads are immutable snapshots. Updating session
defaults publishes a new snapshot, while existing scopes retain their captured
settings. `ReadOnlySet` and `TimeoutSet` distinguish explicit false/zero values
from inheritance.

`Begin` creates a lifecycle scope and applies a timeout context when requested;
it does not itself mutate storage, enforce read-only writes, or change WAL sync
behavior. Those remain executor, authorization, and storage responsibilities.
`Rollback`, `Commit`, and `Close` are concurrency-safe and reject a second
completion.

## Benchmark

The before control constructs the four settings in an ad-hoc map for each
request, which is the allocation-heavy shape callers needed without a shared
session contract. The after run uses the new immutable session snapshot.

| Operation | Median ns/op | B/op | Allocs/op | Relative CPU | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before: ad-hoc settings map | 142.3 | 336 | 2 | 1.00x | 1.00x |
| After: `Resolve` | 10.49 | 0 | 0 | 13.56x faster | 336 B eliminated |
| After: `Begin` + `Rollback` | 45.25 | 64 | 1 | 3.15x faster | 5.25x lower |

Raw five-run samples:

```text
before map ns/op: 140.5, 137.7, 142.3, 144.3, 145.9
before map:       336 B/op, 2 allocs/op
after Resolve ns/op: 9.827, 10.46, 10.49, 10.88, 10.63
after Resolve:       0 B/op, 0 allocs/op
after Begin/Rollback ns/op: 47.18, 44.98, 45.15, 45.25, 45.45
after Begin/Rollback:       64 B/op, 1 alloc/op
```

Reproduce with:

```text
make benchmark-tu05-before
make benchmark-tu05-session-settings
make test-tu05-session-settings
make race-tu05-session-settings
make vet-tu05-session-settings
```

The package-wide gate should also be run through `make test-tu05-package`; the
isolated round29 branch currently has unrelated concurrent M-U05 typed-date and
typed-timestamp failures in that broader test set.
