# TR-017 Single-Source Row Fast Paths

Status: implemented.

This adopts the safe, measurable part of Tarantool's tuple-allocation idea and
ClickHouse's direct projection style. SQL execution already had a scalar
single-source row representation, but several streaming operators rebuilt a
one-entry source map and alias slice for every input row.

The implementation now:

- uses the existing scalar `singleAlias` and `singleRow` fields for direct
  single-source stream operators;
- pre-sizes emitted `SQLRow` maps from the known projection width;
- covers ordered, Top-N, DISTINCT, window, aggregate, spill, and source-filter
  paths;
- leaves multi-source join merge maps unchanged, because those maps carry
  actual relational state and cannot use the scalar representation.

There is no new configuration flag, storage format, wire format, or changed
result ownership rule. This is intentionally not a general arena for tuples
that escape a query; returned rows still own their normal result maps.

## Benchmark

Fixture: 16,384 already-materialized rows, two projected fields, ordered stream
with `LIMIT 32 OFFSET 8192`, five benchmark samples, `-benchmem`.

| Metric | Before | After | Improvement |
| --- | ---: | ---: | ---: |
| Median CPU | 2,063,615 ns/op | 265,601 ns/op | 7.77x faster |
| Median allocated bytes | 4,468,780 B/op | 12,320 B/op | 362.66x lower |
| Median allocations | 49,232 allocs/op | 80 allocs/op | 615.40x lower |

Raw before samples:

```text
2114536 ns/op 4468821 B/op 49232 allocs/op
2063615 ns/op 4468780 B/op 49232 allocs/op
2035876 ns/op 4468772 B/op 49232 allocs/op
2086094 ns/op 4468772 B/op 49232 allocs/op
2018985 ns/op 4468770 B/op 49232 allocs/op
```

Raw after samples:

```text
264992 ns/op 12323 B/op 80 allocs/op
266134 ns/op 12320 B/op 80 allocs/op
266535 ns/op 12320 B/op 80 allocs/op
265601 ns/op 12320 B/op 80 allocs/op
265418 ns/op 12320 B/op 80 allocs/op
```

Reproduce with:

```sh
make test-tr017
make benchmark-tr017-after
```

The focused behavior test also verifies that ordered pagination and projection
remain unchanged and that a single-source execution row does not retain the
map-backed source fields.
