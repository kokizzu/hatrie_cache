# CH-048 `IS NULL` Predicate Kernels

This slice applies the ClickHouse-style typed-column idea to direct SQL
`IS NULL` and `IS NOT NULL` predicates. The columnar matcher recognizes one
field with one NULL operator and reads its existing validity representation
without materializing a per-row interface value.

Supported fast-path layouts are:

- nullable-packed columns (`ColumnarPackedColumn`),
- bit-packed boolean columns (`ColumnarBoolColumn`),
- fixed-width numeric columns (`ColumnarNumericColumn`), and
- ordinary `Columns` slices when no prepared/decompressed representation is
  active.

For typed columns, a nil validity bitmap means every row is non-NULL. A set
validity bit means non-NULL; a clear bit means NULL. The matcher validates row
counts, bitmap lengths, trailing bits, and packed metadata before selecting the
kernel. Dictionaries, list/map/nested columns, malformed layouts, prepared
field layouts, compound expressions, and unsupported aliases retain the
existing evaluator path.

The optimization is internal to query execution. It does not change packing,
wire, persistence, configuration, or SQL semantics.

## Benchmark

Command: `make benchmark-ch048-null`.

Five `-benchmem` samples per case, 4,096 rows, Linux/amd64, AMD Ryzen 9
5950X. Lower is better. The pre-change samples were captured before the
kernel existed. The paired fallback uses the same post-change binary and
fixtures while explicitly invoking the general evaluator.

| Workload | Pre-change median | Paired fallback median | Fast-path median | Improvement vs paired fallback | Allocation change |
| --- | ---: | ---: | ---: | ---: | --- |
| Numeric `IS NULL` | 470,852 ns/op | 434,638 ns/op | 14,685 ns/op | 29.6x faster | 23,042 B/op, 2,880 allocs/op -> 0/0 |
| Numeric `IS NOT NULL` | 458,256 ns/op | 428,663 ns/op | 16,137 ns/op | 26.6x faster | 23,042 B/op, 2,880 allocs/op -> 0/0 |
| Boolean `IS NULL` | 383,167 ns/op | 372,927 ns/op | 15,133 ns/op | 24.6x faster | 1 B/op, 0 allocs/op -> 0/0 |
| Boolean `IS NOT NULL` | 384,531 ns/op | 380,636 ns/op | 15,377 ns/op | 24.8x faster | 1 B/op, 0 allocs/op -> 0/0 |

Raw paired post-change samples:

```text
Fallback/numeric_is_null: 452910 447135 426468 429762 434638 ns/op; 23042 B/op; 2880 allocs/op
Fallback/numeric_is_not_null: 428663 434669 418110 429178 425181 ns/op; 23042 B/op; 2880 allocs/op
Fallback/boolean_is_null: 370280 372927 372376 375434 378265 ns/op; 1 B/op; 0 allocs/op
Fallback/boolean_is_not_null: 385499 374315 376693 380636 392823 ns/op; 1 B/op; 0 allocs/op
FastPath/numeric_is_null: 14317 14860 14685 14318 14687 ns/op; 0 B/op; 0 allocs/op
FastPath/numeric_is_not_null: 16350 16554 16137 15838 15921 ns/op; 0 B/op; 0 allocs/op
FastPath/boolean_is_null: 16924 16280 13364 14881 15133 ns/op; 0 B/op; 0 allocs/op
FastPath/boolean_is_not_null: 13827 15377 16371 16379 13754 ns/op; 0 B/op; 0 allocs/op
```

Focused correctness coverage is in
`hat/hatSql/ch048_null_predicate_test.go`. Reproduce it with
`make test-ch048-null`; the broader CH-048 regression target is
`make test-ch048`.
