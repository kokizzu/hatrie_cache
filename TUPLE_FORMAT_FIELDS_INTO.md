# Tuple format `FieldsInto`

`TupleFormat.FieldsInto` copies schema metadata into caller-owned storage while
preserving the existing independent-copy contract. It is useful for repeated
schema inspection, protocol description, or cache refresh work.

```go
fields := make([]TupleFieldSpec, 0, format.FieldCount())
for range refreshes {
    fields = format.FieldsInto(fields)
    publish(fields)
}
```

The destination is reused when it has sufficient capacity. Default values are
still cloned, so callers cannot mutate the immutable format through the
returned metadata. `Fields()` remains the convenience API and preserves its
empty-format and ownership behavior.

## Measurement

Workload: 64 fixed-width fields, ten benchmark samples on an AMD Ryzen 9
5950X.

| Operation | Median ns/op | B/op | Allocs/op | Relative to pre-change `Fields()` |
| --- | ---: | ---: | ---: | ---: |
| Pre-change `Fields()` | 699.6 | 2,688 | 1 | 1.00x |
| Reused `FieldsInto()` | 357.0 | 0 | 0 | 1.96x faster |

Defaults retain their existing deep-copy cost; the benchmark isolates the
metadata-slice allocation. The API is opt-in and existing callers are
unchanged.
