# C218: `WITH FILL` interpolation

C218 adds opt-in interpolation policies to the existing bounded SQL `WITH FILL`
operator. Existing queries and direct `FillSQLRows` calls with no interpolation
map retain the original fill path.

## SQL

Put `INTERPOLATE` after the `STEP` clause:

```sql
SELECT ts, value, status
FROM CACHE('events')
ORDER BY ts WITH FILL
FROM TIMESTAMP '2026-01-01T00:00:00Z'
TO TIMESTAMP '2026-01-01T00:04:00Z'
STEP DURATION '1m'
INTERPOLATE (value LINEAR, status PREVIOUS)
```

The supported policies are:

| Policy | Generated-row value |
| --- | --- |
| `PREVIOUS` | Value from the preceding observed row; `NULL` before the first observed row. |
| `NEXT` | Value from the following observed row; `NULL` after the last observed row. |
| `LINEAR` | Numeric interpolation between the preceding and following observed rows, returned as `float64`. |

`INTERPOLATE (column)` is shorthand for `PREVIOUS`. Policies apply only to
generated rows. Existing source rows keep their original values. `LINEAR`
returns `NULL` when either endpoint is absent, nonnumeric, invalid, or cannot
produce a finite value. Generated rows between observed rows use the observed
rows as endpoints rather than recursively interpolating earlier generated
values.

The fill column and every interpolated column must be selected by the query.
Column matching is case-insensitive, duplicate selections are rejected, and
unknown policies are rejected. The existing constraints still apply: one
ascending `ORDER BY` item, a positive duration, a half-open `[FROM, TO)` range,
and the configured result-row budget.

## Go API

The direct API uses the same policies:

```go
filled, err := hatSql.FillSQLRows(rows, hatSql.SQLWithFillSpec{
    Column: "at",
    From:   start,
    To:     start.Add(6 * time.Hour),
    Step:   time.Hour,
    Interpolation: map[string]hatSql.SQLWithFillInterpolation{
        "temperature": hatSql.SQLWithFillInterpolationLinear,
        "region":      hatSql.SQLWithFillInterpolationPrevious,
    },
})
```

The interpolation map is optional and defaults to disabled. Direct API column
names are resolved as supplied; SQL aliases are resolved against the projected
output columns.

## Verification and cost

Correctness coverage is in
`hat/hatSql/c218_with_fill_interpolation_test.go`, including direct API
policies, SQL projection, boundaries, unsupported numeric endpoints, malformed
configuration, and parser rejection cases.

The focused benchmark compares the unchanged no-interpolation path with one
linear numeric column over a 100-hour fill range:

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| No interpolation | 16,338 | 38,112 | 305 |
| One `LINEAR` column | 21,937 | 38,120 | 306 |

Enabling one interpolation policy costs `1.34x` CPU time in this small
fill-path benchmark, with only 8 additional bytes and one additional
allocation per call. This is an opt-in feature cost; the default path does not
pay it. Reproduce it with:

```sh
make benchmark-c218
```
