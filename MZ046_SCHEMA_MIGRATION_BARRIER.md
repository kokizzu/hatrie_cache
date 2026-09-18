# MZ-046 Schema Migration Barrier

This adds an opt-in coordination protocol for schema changes that affect
multiple dataflows. A caller prepares a version and named dependencies, each
dependent dataflow acknowledges that exact version, and the caller commits
only after every dependency is ready. Abort and `Forget` support failed or
completed migrations without retaining unbounded state.

The barrier does not store schemas, apply DDL, pause existing tables, or change
default query behavior. Callers remain responsible for applying and validating
the schema. The registry is bounded and snapshots are deterministic.

## Example

```go
barrier, err := hatPipeline.NewSchemaMigrationBarrier(hatPipeline.SchemaMigrationBarrierOptions{})
if err != nil {
	panic(err)
}

_, err = barrier.Prepare(hatPipeline.SchemaMigrationBarrierSpec{
	ID:           "orders-v2",
	Version:      2,
	Dependencies: []string{"reader", "sink"},
})
if err != nil {
	panic(err)
}

// Each dependent dataflow calls this after it has applied version 2.
if err := barrier.Acknowledge("orders-v2", "reader", 2); err != nil {
	panic(err)
}
if err := barrier.Acknowledge("orders-v2", "sink", 2); err != nil {
	panic(err)
}
if _, err := barrier.Commit("orders-v2", 2); err != nil {
	panic(err)
}
```

`Acknowledge` is the allocation-free normal path and is idempotent. Use
`AcknowledgeStatus` or `Status` when a detached diagnostic snapshot is needed.
Versions and dependency names are bounded; dependency lists are normalized,
deduplicated, and sorted.

## Measurement

Run:

```text
make benchmark-mz046-schema-migration-barrier
```

The five-sample benchmark uses four dependencies on an AMD Ryzen 9 5950X.
The direct set is a lower-bound comparison, not a replacement implementation.

| Path | Samples (ns/op) | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Direct dependency set | 21.73, 21.28, 18.97, 19.08, 21.19 | 21.19 | 0 | 0 |
| Barrier `Acknowledge` | 59.85, 56.23, 56.45, 59.31, 53.30 | 56.45 | 7 | 0 |

The bounded mutex-protected protocol is 2.66x the direct-set lower bound, but
adds no heap allocations. An initial status-returning acknowledgement measured
257.5 ns/op, 240 B/op, and 3 allocations; splitting the allocation-free normal
path from diagnostic snapshots reduced that to 56.45 ns/op, 7 B/op, and zero
allocations.
