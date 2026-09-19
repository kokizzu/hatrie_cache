# CH-U44 SQL Aggregate Combinators

CH-U44 adds filtered forms for the built-in SQL aggregate state and merge
functions. The implementation composes the existing aggregate `FILTER`
execution path, so it does not introduce a second state codec or a new wire
format.

## Supported Syntax

State-producing functions accept their normal arguments followed by a boolean
condition:

| Function family | Input | Output |
| --- | --- | --- |
| `COUNT_STATE_IF`, `SUM_STATE_IF`, `AVG_STATE_IF`, `MIN_STATE_IF`, `MAX_STATE_IF` | Aggregate arguments, condition | Serialized aggregate state as `[]byte` |
| `ARGMAX_STATE_IF`, `ARGMIN_STATE_IF` | Value, ordering value, condition | Serialized arg-extreme state as `[]byte` |
| `COUNT_MERGE_IF`, `SUM_MERGE_IF`, `AVG_MERGE_IF`, `MIN_MERGE_IF`, `MAX_MERGE_IF` | State, condition | Final scalar aggregate value |
| `ARGMAX_MERGE_IF`, `ARGMIN_MERGE_IF` | State, condition | Selected value from the merged states |

The condition is evaluated for each input row for `*_STATE_IF`. For
`*_MERGE_IF`, it is evaluated for each input partial state before that state is
merged.

## Examples

Build a partial sum using only rows whose condition is true:

```sql
FROM VALUES (1, true), (2, false), (5, true) AS events(amount, keep)
SELECT SUM_STATE_IF(events.amount, events.keep) AS state
```

The `state` column is a non-empty serialized `[]byte`. Merge it with other
partial states:

```sql
FROM VALUES ($1, true), ($2, false) AS partial(state, keep)
SELECT SUM_MERGE_IF(partial.state, partial.keep) AS amount
```

With the first example's state and a second state containing `2`, the result is
`6` when the second partial state is excluded.

The combinator can be combined with the existing SQL filter syntax. Conditions
are ANDed:

```sql
FROM VALUES (1, true), (2, true), (5, false) AS events(amount, keep)
SELECT SUM_STATE_IF(events.amount, events.keep)
       FILTER (WHERE events.amount > 1) AS state
```

This state contains only the value `2`.

Arg-extreme states use the same value/order argument contract as
`ARGMAX_STATE` and `ARGMIN_STATE`:

```sql
FROM VALUES ('alpha', 10, true), ('beta', 20, false), ('gamma', 30, true)
  AS events(payload, score, keep)
SELECT ARGMAX_STATE_IF(events.payload, events.score, events.keep) AS state
```

Merging that state with `ARGMAX_MERGE_IF(state, true)` returns `gamma`.

## Validation And Compatibility

- State and merge arity is validated after removing only the final condition.
- A `*` condition is rejected with a parser diagnostic.
- Window forms of these aggregate combinators are rejected, matching the
  existing `If` aggregate behavior.
- Existing `STATE`, `MERGE`, `*_IF`, and `FILTER (WHERE ...)` behavior remains
  unchanged.
- State bytes use the existing built-in aggregate state serialization. No new
  persistence or transfer format is introduced.
- The SQL syntax covers the built-in state families; registering a custom
  `SQLAggregateCombinator` does not automatically create SQL parser names.

## Measurement

The benchmark uses a deterministic 128-row `VALUES` source and executes the
query through the same parser and executor path. Values below are five raw
`-benchmem` samples on Linux/amd64 with an AMD Ryzen 9 5950X. The clean
baseline was captured before the feature; the two after rows were captured by
`make benchmark-chu44` after the implementation was optimized to share the
existing aggregate normalization pass.

| Operation | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | CPU vs clean baseline |
| --- | --- | ---: | ---: | ---: | ---: |
| Before: existing `SUM_STATE(...) FILTER` | 86,366; 87,541; 89,298; 89,633; 94,293 | 89,298 | 115,768 | 608 | 1.00x |
| After: existing `SUM_STATE(...) FILTER` control | 86,327; 93,376; 86,419; 88,605; 91,161 | 88,605 | 115,768 | 608 | 0.99x |
| After: `SUM_STATE_IF(...)` | 87,924; 87,663; 89,543; 92,229; 92,607 | 89,543 | 115,768 | 608 | 1.00x (0.27% slower) |

The new spelling has the same measured heap and allocation profile as the
existing filtered spelling. Its median CPU cost is within benchmark noise of
the clean baseline; the small 0.27% difference is not treated as a meaningful
regression. The implementation adds no retained state memory and leaves the
serialized state bytes unchanged.
