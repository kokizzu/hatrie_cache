# SQL Dataflow Fragment Executor

`CompileSQLDataflow` provides a small execution boundary for a lowered
`SQLDataflowPlan`. It validates and snapshots the plan, then invokes a caller
owned runner once for each fragment in ID order. The runner supplies the
operator semantics, so this API can be used by typed, incremental, or
differential query components without changing the existing SQL executor.

## Basic Usage

The usual entry point is a compiled query. Compilation happens once; each
`Execute` call supplies a new input snapshot:

```go
compiled, err := hatSql.CompileSQLQuery(
    "FROM VALUES (1) AS src(id) SELECT src.id",
)
if err != nil {
    return err
}

executor, err := compiled.CompileDataflow(func(
    ctx context.Context,
    fragment hatSql.SQLDataflowFragment,
    inputs hatSql.SQLDataflowFragmentInputs,
) ([]hatSql.SQLRow, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    rows := inputs.Initial()
    if inputs.Len() > 0 {
        rows = inputs.Rows(0)
    }
    // Apply the operator represented by fragment.Kind here.
    return rows, nil
})
if err != nil {
    return err
}

rows, err := executor.Execute(ctx, []hatSql.SQLRow{{"id": int64(1)}})
```

The runnable external-package example is in
`hat/hatSql/m052b_dataflow_fragment_example_test.go`.

## Contract

- `CompileDataflow` lowers a `CompiledSQLQuery`, validates the plan, and
  retains an independent snapshot. `CompileSQLDataflow` accepts a plan from
  `LowerDataflow` when a caller needs to compose or transform it first.
- Fragment IDs must be contiguous and match their positions. Every input must
  refer to an earlier fragment, which rejects cycles and forward references.
- `SQLDataflowFragmentInputs.Initial` returns the input passed to `Execute`.
  `Len`, `FragmentID`, and `Rows` expose the already-produced upstream outputs
  without constructing an input slice for every callback.
- The runner must treat the fragment, input rows, and input metadata as
  read-only during the callback. It owns the returned row slice. An executor
  can be reused, including concurrently, when the runner itself is safe for
  concurrent calls.
- A canceled context is rejected before execution, checked before each
  fragment, and checked after each callback. Runner errors are wrapped with
  the fragment ID and kind while preserving `errors.Is` matching.
- The API does not silently execute SQL expressions. The runner must implement
  the semantics it needs. Existing `Execute` and `ExecuteRows` behavior,
  defaults, storage, wire formats, and backup formats are unchanged.

An empty validated plan returns no rows and no error. Nil runners, malformed
plans, nil contexts, and nil executors return explicit errors.

## Built-In Native Batch Path

`CompiledSQLQuery.CompileNativeDataflow` is an opt-in built-in executor for a
small, allocation-conscious dataflow shape. It accepts one already-resolved
`CACHE` or `KEYS` batch and fuses the scalar `WHERE` predicate with scalar
`SELECT` projection:

```go
compiled, err := hatSql.CompileSQLQuery(
    "FROM CACHE('items') AS src SELECT src.id, src.value WHERE src.value >= 2048",
)
if err != nil {
    return err
}

executor, err := compiled.CompileNativeDataflow()
if err != nil {
    return err
}

rows, err := executor.Execute(ctx, inputRows)
```

The native handle is reusable. It checks the context before each input row,
validates typed source fields, preserves row order, duplicates, and SQL
`NULL` filtering semantics, and does not mutate the input rows. Scalar
evaluation errors retain the normal SQL behavior. `ErrSQLNativeDataflowUnsupported`
is returned at compilation time for joins, unions, CTEs, mixed aggregate and
non-aggregate projections, unsupported grouping or `DISTINCT` shapes, window functions, ordering, limits,
samples, `DISTINCT`, `*`, custom function calls, and other shapes outside the
supported built-in paths. Callers use the ordinary `CompiledSQLQuery.Execute`
or callback-backed `CompileDataflow` path for those queries.

Aggregate-only single-source queries are also supported when every selected
expression is one of `COUNT`, `SUM`, `AVG`, `MIN`, or `MAX`, with an optional
scalar `WHERE` predicate and no `GROUP BY`, `ORDER BY`, `LIMIT`, `OFFSET`,
`DISTINCT`, aggregate `FILTER`, window, join, or custom function. The native
executor returns one row, including an empty-input result with SQL-compatible
`COUNT` and `NULL` results for aggregates without values. It evaluates the
same typed source and SQL null semantics as the ordinary global aggregate
executor.

Grouped single-source queries are supported when they have one directly
selected integer `GROUP BY` field and all other selected expressions are the
same built-in aggregates. The native path preserves first-seen group order,
duplicate rows, and one SQL `NULL` group. It accepts signed and unsigned
integer source values that fit in `int64`; other runtime group-key types return
`ErrSQLNativeDataflowUnsupported` so callers can use a different executor.

Distinct-only single-source queries are supported when one directly selected
integer field is deduplicated, with an optional scalar `WHERE` predicate. The
native path preserves first-seen order and one SQL `NULL` value, accepts
signed and unsigned integers that fit in `int64`, and returns
`ErrSQLNativeDataflowUnsupported` for other runtime key types.

This API does not resolve sources, change query defaults, or replace the
ordinary SQL executor. It is a lower-level boundary for callers that already
own a consistent source batch; the source-resolution and snapshot contract
remain the caller's responsibility.

## Measurement

Command:

```text
make benchmark-m052b-dataflow-fragments
```

The benchmark transforms 128 map rows through five identical stages. Plan
compilation is outside the timed loop. The direct path and the executor path
run the same row transformation. Results below are the medians of five runs
on Linux/amd64 with an AMD Ryzen 9 5950X:

| Path | Median ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| Direct five-stage loop | 144,089 | 220,841 | 1,290 |
| Reusable fragment executor | 155,126 | 220,969 | 1,291 |

Raw timed runs were `143254, 144089, 141378, 144316, 145553 ns/op` for
direct and `145037, 147087, 155126, 157046, 156084 ns/op` for the executor.
The executor uses `1.077x` the paired direct CPU median, with `+128 B/op` and
`+1 alloc/op` for the per-execution dependency-output table. This is an
opt-in extensibility cost, not a claim that the callback boundary is faster
than an inlined loop. A direct-only pre-implementation run measured
`141098 ns/op`, `220841 B/op`, and `1290 allocs/op`; the paired control is used
for the comparison because separate benchmark invocations showed normal CPU
noise.

### Native Batch Measurement

Command:

```text
make benchmark-m052c-native-dataflow
```

The paired benchmark compiles the same single-source query, evaluates it over
4,096 map rows, and runs five samples for the ordinary compiled executor and
the opt-in native batch executor. Compilation is outside the timed loop. The
native path receives the same resolved rows directly, so the comparison
measures query execution overhead rather than source storage or wire transfer.

| Path | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Ordinary compiled executor | 1,673,810 | 2,657,606 | 12,304 | baseline |
| Native scalar batch executor | 899,440 | 720,928 | 4,098 | 1.86x faster; 3.69x fewer bytes; 3.00x fewer allocations |

Raw paired samples:

| Run | Ordinary ns/op | Native ns/op | Ordinary B/op | Native B/op | Ordinary allocs/op | Native allocs/op |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,673,810 | 904,438 | 2,657,605 | 720,928 | 12,304 | 4,098 |
| 2 | 1,667,308 | 883,108 | 2,657,613 | 720,928 | 12,304 | 4,098 |
| 3 | 1,659,304 | 883,223 | 2,657,615 | 720,928 | 12,304 | 4,098 |
| 4 | 1,688,848 | 906,509 | 2,657,606 | 720,928 | 12,304 | 4,098 |
| 5 | 1,687,189 | 899,440 | 2,657,606 | 720,928 | 12,304 | 4,098 |

The gain is conditional on the caller already having a source batch and on
the query fitting the supported scalar shape. Unsupported plans fail closed
instead of silently taking a partial semantic path, and the ordinary executor
remains the compatibility fallback.

### Native Global Aggregate Measurement

Command:

```text
make benchmark-m052d-native-aggregate
```

The paired benchmark evaluates five global aggregates over 4,096 already
resolved rows, with a scalar `WHERE` predicate. Compilation is outside the
timed loop. The ordinary compiled executor and native aggregate executor use
the same rows and five paired samples.

| Path | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Ordinary compiled executor | 1,920,319 | 2,996,500 | 16,475 | baseline |
| Native global aggregate executor | 1,122,827 | 1,379,473 | 12,317 | 1.71x faster; 2.17x fewer bytes; 1.34x fewer allocations |

Raw paired samples:

| Run | Ordinary ns/op | Native ns/op | Ordinary B/op | Native B/op | Ordinary allocs/op | Native allocs/op |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,829,136 | 1,122,827 | 2,996,837 | 1,379,473 | 16,478 | 12,317 |
| 2 | 1,944,206 | 1,087,612 | 2,996,500 | 1,379,444 | 16,475 | 12,317 |
| 3 | 1,805,740 | 1,102,521 | 2,996,626 | 1,379,474 | 16,476 | 12,317 |
| 4 | 1,920,319 | 1,130,396 | 2,996,483 | 1,379,449 | 16,475 | 12,317 |
| 5 | 1,930,984 | 1,151,369 | 2,996,477 | 1,379,498 | 16,475 | 12,317 |

The pre-implementation ordinary-only control measured 2,052,401 ns/op,
2,996,887 B/op, and 16,479 allocations/op across five samples. The paired
control is used for the reported ratio because it runs beside the native path
and avoids treating separate benchmark invocations as a code-path change.
The optimization is limited to the aggregate-only and narrow grouped shapes;
ordinary SQL execution remains the compatibility path for richer queries.

### Native Grouped Aggregate Measurement

Command:

```text
make benchmark-m052e-native-group
```

The benchmark groups 4,096 already resolved rows into 257 integer-key groups,
applies a scalar `WHERE` predicate, and computes six built-in aggregates. Both
paths compile the query outside the timed loop and receive the same rows. The
ordinary executor and native grouped executor each use five paired
`-benchmem` samples on Linux/amd64 with an AMD Ryzen 9 5950X.

| Path | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Ordinary compiled executor | 2,852,423 | 2,792,071 | 16,600 | baseline |
| Native grouped aggregate executor | 1,370,500 | 1,278,722 | 8,099 | 2.08x faster; 2.18x fewer bytes; 2.05x fewer allocations |

Raw paired samples:

| Run | Ordinary ns/op | Native ns/op | Ordinary B/op | Native B/op | Ordinary allocs/op | Native allocs/op |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2,866,719 | 1,337,656 | 2,792,071 | 1,278,847 | 16,600 | 8,100 |
| 2 | 2,941,904 | 1,371,792 | 2,792,394 | 1,278,722 | 16,602 | 8,099 |
| 3 | 2,839,091 | 1,370,500 | 2,791,934 | 1,278,772 | 16,599 | 8,100 |
| 4 | 2,730,815 | 1,364,299 | 2,792,123 | 1,278,654 | 16,600 | 8,099 |
| 5 | 2,852,423 | 1,373,245 | 2,791,799 | 1,278,673 | 16,599 | 8,099 |

The pre-implementation ordinary-only baseline median was `2,881,364 ns/op`,
`2,792,011 B/op`, and `16,600 allocs/op`. The paired control is reported for
the ratio because it controls for normal benchmark noise. No storage, wire,
or default-execution behavior changes.

### Native Ordered Limit Measurement

Command:

```text
make benchmark-m052h-native-ordered-limit
```

The benchmark applies `WHERE value >= 0 ORDER BY value DESC LIMIT 32 OFFSET
512` to 4,096 already resolved rows and projects two fields. Both paths compile
the query outside the timed loop and receive the same rows. The ordinary
executor and native ordered executor each use five paired `-benchmem` samples
on Linux/amd64 with an AMD Ryzen 9 5950X. The native path retains only the
bounded Top-N candidate set and evaluates projection expressions after the
final page is selected. Its current opt-in shape requires one direct qualified
source-field ordering expression; projected aliases and other unsupported
ordering shapes fail closed.

| Path | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Ordinary compiled executor | 7,726,223 | 3,784,054 | 20,605 | baseline |
| Native ordered executor | 1,796,266 | 106,119 | 637 | 4.30x faster; 35.66x fewer bytes; 32.35x fewer allocations |

Raw paired samples:

| Run | Ordinary ns/op | Native ns/op | Ordinary B/op | Native B/op | Ordinary allocs/op | Native allocs/op |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 7,726,223 | 1,796,266 | 3,784,126 | 106,132 | 20,607 | 637 |
| 2 | 7,806,351 | 1,799,055 | 3,784,329 | 106,119 | 20,608 | 637 |
| 3 | 7,839,020 | 1,771,476 | 3,783,810 | 106,170 | 20,603 | 637 |
| 4 | 7,716,618 | 1,805,342 | 3,783,900 | 106,068 | 20,605 | 636 |
| 5 | 7,621,486 | 1,786,047 | 3,784,054 | 106,106 | 20,605 | 637 |

The pre-implementation ordinary-only baseline median was `7,739,188 ns/op`,
`3,783,806 B/op`, and `20,604 allocs/op`. The paired control is reported for
the ratio because it controls for normal benchmark noise. The feature is
opt-in and changes no storage, wire, or default SQL behavior.

### Native Limit Measurement

Command:

```text
make benchmark-m052g-native-limit
```

The benchmark applies `WHERE value >= 0 LIMIT 32 OFFSET 512` to 4,096 already
resolved rows and projects two fields. Both paths compile the query outside the
timed loop and receive the same rows. The ordinary executor and native scalar
executor each use five paired `-benchmem` samples on Linux/amd64 with an AMD
Ryzen 9 5950X. The native path stops after the post-filter page and therefore
does not project the remaining rows. Finite windows are supported only for the
plain scalar projection path; grouped, aggregate, and `DISTINCT` windows still
fail closed to preserve their existing materialization semantics.

| Path | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Ordinary compiled executor | 2,269,821 | 3,481,020 | 16,428 | baseline |
| Native scalar executor | 64,618 | 11,230 | 67 | 35.13x faster; 309.98x fewer bytes; 245.19x fewer allocations |

Raw paired samples:

| Run | Ordinary ns/op | Native ns/op | Ordinary B/op | Native B/op | Ordinary allocs/op | Native allocs/op |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2,287,573 | 64,398 | 3,481,020 | 11,232 | 16,428 | 67 |
| 2 | 2,261,348 | 64,395 | 3,480,726 | 11,229 | 16,426 | 67 |
| 3 | 2,287,249 | 65,092 | 3,481,103 | 11,230 | 16,429 | 67 |
| 4 | 2,239,096 | 64,618 | 3,481,071 | 11,230 | 16,429 | 67 |
| 5 | 2,269,821 | 65,230 | 3,480,886 | 11,230 | 16,428 | 67 |

The pre-implementation ordinary-only baseline median was `2,246,740 ns/op`,
`3,481,030 B/op`, and `16,428 allocs/op`. The paired control is reported for
the ratio because it controls for normal benchmark noise. The feature is
opt-in and changes no storage, wire, or default SQL behavior.

### Native Distinct Measurement

Command:

```text
make benchmark-m052f-native-distinct
```

The benchmark deduplicates one integer field from 4,096 already resolved rows
after a scalar `WHERE` predicate. Both paths compile the query outside the
timed loop and receive the same rows. The ordinary executor and native
distinct executor each use five paired `-benchmem` samples on Linux/amd64 with
an AMD Ryzen 9 5950X.

| Path | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Ordinary compiled executor | 2,377,985 | 2,979,083 | 22,243 | baseline |
| Native distinct executor | 484,782 | 267,616 | 538 | 4.91x faster; 11.13x fewer bytes; 41.34x fewer allocations |

Raw paired samples:

| Run | Ordinary ns/op | Native ns/op | Ordinary B/op | Native B/op | Ordinary allocs/op | Native allocs/op |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2,330,361 | 475,529 | 2,979,083 | 267,612 | 22,243 | 538 |
| 2 | 2,265,494 | 484,782 | 2,979,148 | 267,622 | 22,243 | 538 |
| 3 | 2,381,125 | 497,580 | 2,978,741 | 267,614 | 22,240 | 538 |
| 4 | 2,377,985 | 493,976 | 2,978,994 | 267,641 | 22,242 | 538 |
| 5 | 2,419,058 | 480,919 | 2,979,143 | 267,616 | 22,243 | 538 |

The pre-implementation ordinary-only baseline median was `2,418,758 ns/op`,
`2,979,199 B/op`, and `22,243 allocs/op`. The paired control is reported for
the ratio because it controls for normal benchmark noise. The feature is
opt-in and changes no storage, wire, or default SQL behavior.
