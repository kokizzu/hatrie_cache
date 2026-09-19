# M-U44 Cross-Dataflow Transaction Visibility

M-U44 is an opt-in publication barrier for independent maintained SQL views.
It gives a caller one logical version for a named batch so readers can reject
mixed-version observations.

## Contract

`SQLDataflowVisibilityCoordinator` stores only a bounded name-to-version map.
It does not execute SQL, retain rows, copy view state, or start a worker.

1. Register each maintained view with `Register`.
2. Prepare all named views in the caller's own transaction or update path.
3. Call `Publish` once with the complete batch. The coordinator assigns one
   new logical version and advances every named view atomically under one lock.
4. Call `Acquire` before a multi-view read. It returns a token only when all
   requested views currently have the same version.
5. Call `Check` immediately before or during the read. A later publication
   makes the token stale.

`Publish` is intentionally explicit. It cannot verify that an external view
has been prepared, so callers must not publish a name before its corresponding
view state is ready. The coordinator is a visibility fence, not a substitute
for a distributed commit protocol or historical row snapshots.

## Example

```go
visibility, err := hatSql.NewSQLDataflowVisibilityCoordinator(
	 hatSql.SQLDataflowVisibilityOptions{},
)
if err != nil {
	return err
}
for _, name := range []string{"orders", "customers"} {
	if err := visibility.Register(name); err != nil {
		return err
	}
}

// The caller prepares both maintained views before this call.
if _, err := visibility.Publish([]string{"orders", "customers"}); err != nil {
	return err
}
token, err := visibility.Acquire([]string{"orders", "customers"})
if err != nil {
	return err // ErrSQLDataflowVisibilityNotReady means retry later.
}
if err := visibility.Check(token); err != nil {
	return err // A concurrent publication made this token stale.
}
```

The zero-value option uses a maximum of 1,024 registered dataflows. A
coordinator is disabled by omission: existing SQL/materialized-view paths do
not construct or consult one. `Snapshot` and `Restore` provide a deterministic
metadata checkpoint; restore validates names, versions, duplicates, and the
configured capacity before replacing state.

## Measured Cost

Five `-count=5` local samples on Linux/amd64, AMD Ryzen 9 5950X. The
four-dataflow coordinator workload uses names, a mutex, validation, sorting,
and a defensive token copy. The baselines only update four preallocated
numeric slots and therefore are lower bounds, not equivalent consistency
guarantees.

| Workload | Median ns/op | B/op | allocs/op | CPU vs one-lock baseline |
| --- | ---: | ---: | ---: | ---: |
| Baseline, one lock, four slots | 3.872 | 0 | 0 | 1.00x |
| Baseline, four independent locks | 15.00 | 0 | 0 | 3.88x |
| `Publish` four dataflows | 191.2 | 64 | 1 | 49.38x |
| `Acquire` four dataflows | 133.6 | 64 | 1 | 34.50x |
| `Check` four dataflows | 128.2 | 64 | 1 | 33.11x |
| `Snapshot` four dataflows | 289.2 | 192 | 4 | 74.69x |

This is a correctness/operational capability, not a fast path. The measured
cost is paid only by callers that opt in, while the default SQL path has no
additional per-operation work or memory. The raw benchmark commands are:

```sh
make benchmark-mu44-baseline
make benchmark-mu44
```

## Verification

```sh
make test-mu44
make verify-mu44
```

The focused tests cover common-version publication, mixed-version rejection,
stale tokens, capacity and malformed input, atomic restore failure behavior,
and concurrent publication under the race detector.
