# SQL Vectorized Execution

`hatrie_cache` adopts a narrow ClickHouse-style vectorized execution idea for
grouped SQL over immutable columnar source batches. It is an internal physical
plan; callers continue to use the existing SQL APIs and
`ColumnarSourceResolver` interface.

## What It Does

For a single `CACHE` source, the executor reads aligned column slices in fixed
blocks of 1,024 rows. It evaluates the `WHERE` predicate into a reusable
selection vector, then updates one compact aggregate state per group directly
from the selected column positions. Rejected rows never become `sqlExecRow`
values or source maps.

The path is admitted automatically only when all of these conditions hold:

- one direct group field;
- direct built-in `COUNT`, `SUM`, `AVG`, `MIN`, or `MAX` projections;
- a columnar source resolver and a predicate shape supported by the existing
  columnar predicate analyzer;
- no joins, CTEs, unions, `HAVING`, `ORDER BY`, `DISTINCT`, windows, subqueries,
  samples, typed source fields, or active `MaxGroupBytes` budget.

Dictionary-backed group fields can still use the existing specialized
dictionary aggregate path. `QueryRows` uses the vectorized path for the
generic columnar grouped shape as well, including dictionary-backed values.

## Compatibility

The physical path preserves the established SQL behavior:

- first-seen group order when no `ORDER BY` is present;
- one group for NULL keys, with the first NULL value retained;
- SQL NULL behavior for `COUNT(field)`, `SUM`, `AVG`, `MIN`, and `MAX`;
- `OFFSET`, `LIMIT`, `MaxGroupRowsPerKey`, `MaxResultBytes`, context checks, and
  callback errors;
- source row-count and column-length validation.

Any unsupported shape, unavailable columnar batch, invalid batch, or configured
group-memory budget returns to the existing executor. There is no new public
API, configuration flag, storage format, wire format, or backup/restore
behavior.

## Tradeoffs

The reusable selection vector has capacity for at most 1,024 machine `int`
positions, about 8 KiB on a 64-bit build. The path also retains a group-key map
and one aggregate state per distinct group, while the old path materializes
source row maps for the whole query. The selection buffer is per query and is
released with the query; it is not a per-row allocation and is not persistent
metadata.

The benchmark's `B/op` column is cumulative allocation volume, not RSS or live
retained heap. The source batch is prepared before the timed operation, and
both paths include SQL parsing and result materialization in the timed query.
The automatic path was faster and allocated less in all measured input sizes;
unsupported queries retain the old path.

## Verification

The focused tests compare vectorized results with the row executor, cover NULL
keys and values, filters, block boundaries, dictionary keys, callbacks,
fallback ordering, and group-skew limits:

```sh
make test-sql-vectorized
make test-race-sql-vectorized
make benchmark-sql-vectorized-long
```

The design follows the fixed-batch and selection-vector principles described in
[ClickHouse vectorized query execution](https://clickhouse.com/resources/engineering/vectorized-query-execution).
