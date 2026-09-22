# M223 Materialized View Hydration

M223 adds an explicit lifecycle for maintained views that are registered before
their first result is available. The lifecycle distinguishes `cold`,
`hydrating`, and `ready` without changing the existing synchronous `Create`
path.

## API

```go
views := hatSql.NewMaterializedViews()
definition := hatSql.MaterializedViewDefinition{
	Name:         "people_view",
	Query:        "FROM CACHE('people') SELECT id, name",
	Dependencies: []string{"people"},
}

status, err := views.CreateCold(definition)
// status.HydrationState == hatSql.MaterializedViewHydrationStateCold

status, err = views.Hydrate(ctx, "people_view", resolver, hatSql.QueryOptions{})
// status.HydrationState == hatSql.MaterializedViewHydrationStateReady

err = views.MarkMaterializedViewCold("people_view")
// The retained result is withdrawn until Hydrate succeeds again.
```

`Create` remains the compatibility path and publishes a `ready` view in one
synchronous call. `CreateCold` only registers and validates the definition;
it does not call the source resolver.

## State Machine

| Current | Operation | Next | Behavior |
| --- | --- | --- | --- |
| cold | `Hydrate` starts | hydrating | The query runs outside the registry lock. |
| hydrating | query and index build succeed | ready | Result and maintained point indexes publish atomically. |
| hydrating | query, budget, or index build fails | cold | No partial result is published; retry is allowed. |
| ready | `MarkMaterializedViewCold` | cold | Retained rows and accounting are withdrawn. |
| hydrating | `MarkMaterializedViewCold` | unchanged | Rejected with `ErrMaterializedViewHydrationInProgress`. |
| ready | `Hydrate` | unchanged | Rejected with `ErrMaterializedViewHydrationNotCold`. |

`Get` exposes the in-progress state. `LookupPoint`, point-lookup reader
acquisition, exact maintained-projection hits, and `RefreshChanged` reject
`cold` and `hydrating` views with `ErrMaterializedViewHydrationNotReady`.
Existing point-lookup reader leases retain their immutable snapshots so a
reader already admitted before cold fencing can drain safely.

The generation check at refresh publication prevents an update that started
before `MarkMaterializedViewCold` from resurrecting a cold view. The same
generation change invalidates an in-flight background point-lookup build.

This is a process-local lifecycle API. It does not add persistence, source
offset recovery, or automatic scheduling; those remain caller responsibilities.

## Measured Cost

Workload: one 256-row `people` source, one maintained projection, five samples,
`-benchtime=100ms`, `-benchmem`, Linux amd64, AMD Ryzen 9 5950X.

Raw samples:

```text
BenchmarkM223MaterializedViewCreateControl-32  358  310948 ns/op  270958 B/op  1567 allocs/op
BenchmarkM223MaterializedViewCreateControl-32  411  278094 ns/op  270955 B/op  1567 allocs/op
BenchmarkM223MaterializedViewCreateControl-32  470  246626 ns/op  270955 B/op  1567 allocs/op
BenchmarkM223MaterializedViewCreateControl-32  536  260847 ns/op  270956 B/op  1567 allocs/op
BenchmarkM223MaterializedViewCreateControl-32  510  260749 ns/op  270955 B/op  1567 allocs/op

BenchmarkM223MaterializedViewColdHydrate-32    415  241179 ns/op  270971 B/op  1568 allocs/op
BenchmarkM223MaterializedViewColdHydrate-32    312  367718 ns/op  270974 B/op  1568 allocs/op
BenchmarkM223MaterializedViewColdHydrate-32    454  245693 ns/op  270972 B/op  1568 allocs/op
BenchmarkM223MaterializedViewColdHydrate-32    411  250793 ns/op  270972 B/op  1568 allocs/op
BenchmarkM223MaterializedViewColdHydrate-32    387  263006 ns/op  270972 B/op  1568 allocs/op
```

Median comparison:

| Path | ns/op | B/op | allocs/op | Feature/control |
| --- | ---: | ---: | ---: | ---: |
| Existing synchronous `Create` | 260,847 | 270,956 | 1,567 | 1.00x |
| `CreateCold` + `Hydrate` | 250,793 | 270,972 | 1,568 | 0.96x CPU, 1.0001x bytes, 1.0006x allocs |

The measured difference is within normal benchmark noise for this workload.
The opt-in lifecycle added 16 retained benchmark bytes and one allocation per
full initialization; it does not impose a hydration state transition on the
default `Create` path.

## Verification

```text
make test-m223-materialized-hydration
make test-m223-related-materialized
make benchmark-m223-materialized-hydration
make race-m223-materialized-hydration
make vet-m223-materialized-hydration
```

The focused, related, race, vet, and benchmark targets pass. The repository-wide
baseline also contains unrelated existing failures in compact protocol frame
size, peer TLS rotation timeout, and typed-table arrangement checkpoints; the
same failures reproduce at the pre-M223 commit.
