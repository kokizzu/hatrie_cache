# M215 Delta-Join Maintenance Audit

## Decision

M215 is already implemented by `hatSql.IncrementalJoin`. It maintains signed
differential state on both join sides, indexes active rows by equality key, and
emits only the pair deltas caused by each input update. It also handles
multiplicity, replacement updates, overflow checks, atomic rejection, and
randomized reference-model verification.

The operator is the appropriate Materialize-style delta-join implementation
for this repository. Adding another high-churn join API would duplicate state
and semantics without improving the maintained path.

## Correctness

Command:

```text
make test-m215-delta-join-audit
```

Result: all `TestMZ030*` tests passed, including weighted matches, atomic
validation, replacement rows, row cloning, and a 1,000-step randomized
reference comparison.

## Measurements

Five `-benchmem` samples ran on Linux amd64 on an AMD Ryzen 9 5950X. The
workload has 10,000 rows per side and changes one left row on each iteration.

| Path | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Rebuild the 10k-row join | 5,424,235 | 5,530,657 | 60,035 | baseline |
| Existing incremental delta join | 2,238 | 2,071 | 15 | 2,424x faster; 2,671x lower bytes; 4,002x fewer allocations |

Command:

```text
make benchmark-m215-delta-join-audit
```

M215 is therefore closed as already covered. No runtime change was made.
