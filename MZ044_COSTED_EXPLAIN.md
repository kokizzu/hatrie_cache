# MZ-044 Costed SQL EXPLAIN

Status: implemented as an explicit, plan-only diagnostic mode.

Materialize-style cost visibility is available through:

```sql
EXPLAIN COST FROM CACHE('orders') AS o
WHERE o.status = 'open'
SELECT o.id
```

`EXPLAIN COST` keeps the existing plan nodes and adds two nullable fields when
the planner has an estimated row count:

- `estimated_cost`: a comparable CPU-work unit;
- `estimated_memory_bytes`: a bounded working-memory heuristic.

The same fields are available in the returned `ExplainStep` values and through
the importable `CostSQLExplainSteps` helper. Callers that need different units
can provide `SQLExplainCostOptions`.

## Estimation Rules

The default row CPU unit is `1` and the default memory unit is `64` bytes per
estimated row. Operator weights are intentionally simple and deterministic:

| Operator class | CPU weight | Memory weight |
|---|---:|---:|
| Scan, values, projection | 1 | 1 |
| Filter and prewhere | 2 | 1 |
| Aggregate and join | 3-4 | 2 |
| Sort, top-N, and limit-by | 4 | 2 |

The result is a planning heuristic, not a promise of wall-clock nanoseconds or
an allocator profile. Steps without a row estimate omit both fields instead of
inventing precision. Arithmetic saturates at the platform maximum integer.

## Safety And Defaults

- Ordinary queries and ordinary `EXPLAIN` do not run the cost pass.
- `EXPLAIN COST` never resolves a source or executes a query.
- Existing `EXPLAIN ANALYZE` remains measured execution; `EXPLAIN COST ANALYZE`
  adds the cost fields to its existing measured plan.
- The returned plan is detached from the input plan, including the new pointer
  fields.
- No storage format, wire format, scheduler, or result-cache behavior changes.

## Measurement

Command:

```text
make benchmark-mz044-costed-explain
```

Five samples on the same four-row `VALUES` plan:

| Mode | Median CPU | Allocated memory | Allocations | Relative CPU |
|---|---:|---:|---:|---:|
| Regular `EXPLAIN` after change | 10,090 ns/op | 10,505 B/op | 58 | 1.00x |
| `EXPLAIN COST` | 11,226 ns/op | 11,483 B/op | 66 | 1.11x |

The explicit diagnostic mode therefore costs about 11% CPU, 978 bytes, and 8
allocations in this small plan. The ordinary `EXPLAIN` path retained the
pre-change allocation profile exactly (`10,505 B/op`, `58 allocs/op`); its CPU
samples vary with host load and are not treated as a speed claim.

The feature is accepted because its cost is opt-in and buys machine-readable
operator cost and memory guidance. It is not used automatically by the
planner, and the default execution path remains unchanged.

Focused verification:

```text
make test-mz044-costed-explain
make test-mz044-package
make race-mz044-costed-explain
make vet-mz044-costed-explain
```
