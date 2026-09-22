# M221: Isolated Compute Cluster Resource Budgets

M221 is already covered by the existing opt-in `hatSql.SQLClusterAdmission`
controller and its `SQLQueryOptions.ClusterAdmission` integration. A new
runtime implementation would duplicate the existing cluster-level admission
path, so this progress adds a regression test and records the coverage and
cost explicitly.

## Existing coverage

Each named cluster gets its own serving and maintenance ledgers. Each ledger
has independent CPU units, memory bytes, running limits, and queue limits.
Serving work cannot consume maintenance capacity, and work in one named
cluster cannot block work in another named cluster. `Stats` exposes the
per-cluster limits and current usage. The controller remains opt-in, so
existing SQL execution has no cluster-admission cost unless the caller sets
`ClusterAdmission`.

The implementation is in `hat/hatSql/mu023_cluster_admission.go`; the SQL
entry-point integration and workload-group behavior are covered by
`hat/hatSql/ch231_workload_groups.go` and its tests.

## Test-first evidence

`hat/hatSql/m221_cluster_isolation_test.go` fills the missing named-cluster
regression case. It holds the full serving budget for `analytics`, then
successfully admits `dashboard` and verifies both clusters retain their own
configured limits and usage. Existing M-U23 tests cover serving versus
maintenance isolation, cancellation, queue bounds, close behavior, and
release-on-execute.

## Measurement

Commands:

```text
make benchmark-mu023-cluster-admission-baseline
make benchmark-mu023-cluster-admission
```

Linux amd64, AMD Ryzen 9 5950X, five benchmark samples:

| Path | Samples (ns/op) | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Direct callback control | 1.578, 1.597, 1.620, 1.602, 1.412 | 1.597 | 0 | 0 |
| `SQLClusterAdmission.Execute` | 196.6, 181.1, 194.3, 201.3, 185.5 | 194.3 | 96 | 1 |

The opt-in isolation path costs about `121.6x` CPU over the direct callback
control in this intentionally tiny admission-only workload, plus 96 bytes and
one allocation per reservation. That is an admission-control cost, not a
claim of faster query execution. It is accepted because the feature provides
the requested independent budgets and bounded queueing; the default path
remains unchanged and pays none of this cost.

## Verification

```text
make format-m221-cluster-isolation
make test-mu023-cluster-admission
make race-mu023-cluster-admission
make vet-mu023-cluster-admission
```

The new test and the existing M-U23 suite pass. No production runtime change
was necessary for M221; the audit prevents the backlog from being treated as
unimplemented and prevents a duplicate resource-isolation mechanism.
