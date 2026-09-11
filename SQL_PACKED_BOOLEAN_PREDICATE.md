# SQL Packed Boolean Predicate Kernel

`hatSql` can evaluate a narrow class of predicates directly against the
existing bit-packed `ColumnarBoolColumn` representation. The fast path applies
to a direct field/literal comparison using `=`, `!=`, or `<>`:

```sql
SELECT active FROM CACHE('items') WHERE active = true;
SELECT active FROM CACHE('items') WHERE false != active;
```

The kernel reads the value bitmap one bit at a time and checks the optional
validity bitmap before comparing. A NULL validity bit never matches, which
preserves SQL three-valued predicate behavior. Bitmap lengths, row counts, and
unused trailing bits are validated before the kernel is constructed.

Only packed boolean columns use this path. Legacy `ColumnarBatch.Columns`,
malformed packed columns, qualified fields for another source, unsupported
operators, and wider expressions continue through the established evaluator.
There is no new wire or persistence format and no reusable selection-mask
allocation. `PackBooleanColumns` remains explicit, so callers control when the
packed representation is used.

## Verification

The focused regression tests cover all supported operators, NULL rows,
reversed literal/field operands, malformed metadata, unsupported operators,
and the kernel's query lookup boundary:

```sh
make test-m065t-boolean-predicate-kernel
```

The full package, race, and vet checks are exposed through the corresponding
`m065t` Makefile targets.

## Measurement

Seven `250ms` samples were collected on `linux/amd64`, AMD Ryzen 9 5950X,
with `GOMAXPROCS=1`. The benchmark builds a 4,096-row packed boolean column
and executes a direct SQL filter. The baseline is the same benchmark copied
into a detached worktree at the parent revision.

| Metric | Existing evaluator | Packed boolean kernel | Change |
| --- | ---: | ---: | ---: |
| Median CPU time | 1,125,634 ns/op | 360,801 ns/op | 3.12x faster |
| Median allocated bytes | 1,865,233 B/op | 740,024 B/op | 2.52x lower |
| Median allocations | 12,327 allocs/op | 4,127 allocs/op | 2.99x fewer |

The reduction is query CPU and transient heap only. Packed storage itself is
already selected by the existing caller-controlled `PackBooleanColumns` API;
this feature does not change its memory accounting or default behavior.

Raw output is recorded in [BENCHMARK.md](BENCHMARK.md#m065t-sql-packed-boolean-predicate-kernel).
