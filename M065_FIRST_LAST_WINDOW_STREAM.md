# M065 SQL `FIRST_VALUE`/`LAST_VALUE` Streaming

The SQL executor now supports `FIRST_VALUE` and `LAST_VALUE` in both the
materialized window executor and the bounded running-window stream.

The stream is selected automatically by `ExecuteSQLQueryRows` when all of the
following are true:

- the source is direct `VALUES`, direct `CACHE`, or a streaming `EXTERNAL`
  source;
- the window has no `PARTITION BY`, `ORDER BY`, or explicit frame;
- the function has one scalar argument without aggregate, window, or custom
  function calls; and
- the query has no joins, grouping, distinct, or outer ordering.

This is the unpartitioned default frame: qualifying rows are processed in
source order, and each output is emitted immediately. `FIRST_VALUE` retains
one value and `LAST_VALUE` retains the current value, so window state is O(1)
per query. `WHERE` filtering occurs before state updates. NULLs are respected:
a NULL first qualifying value remains the first value, while `LAST_VALUE`
returns the latest qualifying value.

Queries outside this subset remain materialized. `ExecuteSQLQueryRows` keeps
its existing contract and returns an unsupported-stream error for queries that
need retained window state; use `ExecuteSQLQuery` for those forms.

## Benchmark

Command: `make benchmark-m065-first-last-window-next10`.

Fixture: 4,096 integer `VALUES` rows, projecting the source value plus both
window functions. Five benchmark samples were collected with `-benchmem`.
The materialized row is the correctness and performance baseline; the streamed
callback discards each row after it is emitted.

| Path | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Materialized baseline | 273,664,810 | 4,007,866 | 20,597 | 1.00x |
| Final bounded stream | 5,963,358 | 4,118,815 | 53,283 | 45.9x faster |

Raw `ns/op` samples:

- materialized: 273,664,810; 265,068,910; 266,293,655; 293,943,440;
  278,850,421;
- streamed: 5,963,324; 6,177,221; 6,137,763; 5,941,805; 5,963,358.

The final stream is about 2.8% higher in cumulative `B/op` and 2.59x higher
in allocation count than the materialized baseline. `B/op` is cumulative test
allocation, not peak retained heap: the stream does not retain the result-row
slice, but the public `SQLRow` callback still requires one output map per
emitted row. The first prototype used the generic one-row batch evaluator and
measured 6,600,934 ns/op, 5,497,944 B/op, and 65,575 allocs/op; the final
scalar evaluator and single row-byte calculation reduced that to the values
above. The change is kept because it delivers the large CPU win while keeping
cumulative bytes nearly flat, and the allocation tradeoff is explicit.

Focused correctness tests cover stream selection, materialized parity, NULL
values, and filtering. Race and vet checks are wired through the corresponding
M065 Makefile targets.
