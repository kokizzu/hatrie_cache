# LIMIT WITH TIES

`LIMIT n WITH TIES` returns the first `n` rows after `ORDER BY`, plus every immediately following row whose complete `ORDER BY` key equals the key of row `n`. It is useful for rank boundaries where returning only an arbitrary fraction of an equal-valued group would be incorrect.

## Syntax

```sql
FROM CACHE('events') AS event
SELECT event.id, event.score
ORDER BY event.score DESC
LIMIT 10 WITH TIES
```

`WITH TIES` requires a finite non-negative `LIMIT` and an `ORDER BY`. With `OFFSET`, the boundary is the last row after the offset and limit:

```sql
... ORDER BY score DESC LIMIT 10 WITH TIES OFFSET 20
```

This skips 20 rows, selects the next 10, and includes adjacent rows tied with the tenth selected row. `LIMIT 0 WITH TIES` returns no rows.

## Execution

- Existing queries without `WITH TIES` keep their existing indexed and top-N paths.
- The opt-in form uses the ordinary materialized sort path, then expands the boundary using all `ORDER BY` expressions.
- If `MaxSortBytes`, `SpillDirectory`, and `MaxSpillBytes` are configured, the external merge path also preserves the tie boundary.
- Output remains stable for equal keys because the existing stable sort and ordinal spill ordering are retained.
- The feature does not apply to `LIMIT BY`; that syntax remains a separate per-group operation.

## Verification

```text
make test-limit-with-ties-local-clean
make test-limit-with-ties-package-local-clean
make test-limit-with-ties-cache-sql-local-clean
make test-limit-with-ties-race-local-clean
make vet-limit-with-ties-local-clean
make benchmark-limit-with-ties-local-clean
```

The tests cover ordinary ties, offset boundaries, zero limits, forced external spill, the row-streaming API, and invalid unordered/unbounded forms.

## Benchmark

Command:

```text
make benchmark-limit-with-ties-local-clean
```

Environment: Linux amd64, AMD Ryzen 9 5950X, Go benchmark with `-benchmem -count=5`. Both queries sort the same 100 unique keys and return 10 rows; this isolates the opt-in behavior from the cost of returning additional tied rows.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative latency | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| ordinary `LIMIT 10` | 75,253 | 119,720 | 930 | baseline | baseline |
| `LIMIT 10 WITH TIES`, no ties | 70,233 | 107,047 | 628 | 0.93x | 0.89x |

The result is not a universal speed claim: the ordinary query selected a separate top-N fast path, while `WITH TIES` correctly used the materialized path. The important compatibility property is that the feature is explicit and existing queries retain their original plan. A workload that actually returns many ties will naturally spend additional time and memory proportional to the extra result rows.
