# M222: Replicated Compute Workers For Maintained Views

M222 adds an opt-in `MaterializedViewComputeReplicaSet` for high-availability
maintained views and point-lookup indexes. Each replica owns an independent
`MaterializedViews` registry. The set fans refreshes out to healthy replicas,
routes point reads across healthy replicas, and allows an operator or health
controller to fence a failed replica without stopping the remaining workers.

This is intentionally process-local. The caller still owns source ordering,
durable checkpoints, placement, and cross-process transport. It is not a
replacement for consensus or replication WAL.

## Usage

Create the same materialized view and point-lookup definition on each registry,
then register them:

```go
replicas, err := hatSql.NewMaterializedViewComputeReplicaSet(
    hatSql.MaterializedViewComputeReplica{Name: "primary", Views: primaryViews},
    hatSql.MaterializedViewComputeReplica{Name: "standby", Views: standbyViews},
)
if err != nil {
    return err
}

// Drive both compute workers from one source snapshot.
if _, err := replicas.RefreshChanged(ctx, []string{"people"}, resolver, options); err != nil {
    return err
}

result, found, err := replicas.LookupPoint("people_by_id", "42")
```

`SetReplicaAvailable(name, false)` fences a replica from both refresh and read
routing. After the replica is rebuilt or caught up, mark it available and run
`RefreshChanged` before relying on it. Unfencing does not fabricate state.

## Correctness And Failure Behavior

- Refresh reports each dispatched replica independently. A failure on one
  replica is returned after the other healthy replicas have had a chance to
  refresh.
- Lookups try all healthy replicas when one is behind and does not contain the
  requested key. A fenced or unavailable replica is never selected.
- An empty healthy set returns
  `ErrMaterializedViewComputeReplicaNoHealthy` rather than silently serving
  from an unknown state.
- Replica names are bounded and unique, and a set is capped at 16 replicas to
  prevent accidental unbounded in-process memory growth.
- Source consistency remains the caller's responsibility. A resolver used for
  a fanout should expose one consistent source snapshot; use the existing
  source-version and checkpoint mechanisms when updates are durable.

## Measurement

Command:

```text
make benchmark-m222-materialized-compute-replicas
```

The workload refreshes a 256-row maintained view and its point-lookup index on
Linux amd64, AMD Ryzen 9 5950X, with five `-benchmem` samples:

| Path | Samples (ns/op) | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| One replica control | 329,830; 324,239; 301,488; 324,490; 356,164 | 324,490 | 295,498 | 2,067 |
| Two replicated compute workers | 602,010; 672,935; 648,036; 614,248; 638,744 | 638,744 | 591,041 | 4,135 |

Two replicas cost `1.97x` CPU, `2.00x` cumulative bytes, and `2.00x`
allocations in this refresh workload. That is expected duplicated compute and
state, not an optimization. The cost is accepted because the feature provides
read continuity after an explicit worker failure; the default single-registry
path remains unchanged and pays no replica-set overhead.

## Verification

```text
make format-m222-materialized-compute-replicas
make test-m222-related-materialized
make benchmark-m222-materialized-compute-replicas
make race-m222-materialized-compute-replicas
make vet-m222-materialized-compute-replicas
```

The tests cover refresh fanout, failover to a refreshed standby, recovery of a
fenced replica, no-healthy-replica errors, validation, and existing M218-M220
materialized-view behavior.
