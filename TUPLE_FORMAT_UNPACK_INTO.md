# Tuple format `UnpackInto`

`TupleFormat.UnpackInto` decodes a packed tuple into a caller-owned value
buffer. It is useful for repeated Tarantool-style tuple reads where the schema
is fixed and the caller can retain one destination slice.

```go
values := make([]TupleFieldValue, 0, format.FieldCount())
for range tuples {
    values, err = format.UnpackInto(tuple, values)
    if err != nil {
        return err
    }
    consume(values)
}
```

The destination is reset and cleared before decoding, so reused buffers do not
retain old nullable fields. The method preserves `Unpack` validation and
decode errors. `Unpack` remains the ownership-safe convenience API and still
returns `nil` on error; byte/string value ownership follows the existing
decoder behavior.

## Measurement

Workload: five typed fields, measured with `go test -benchmem -count=10` on an
AMD Ryzen 9 5950X.

| Operation | Median ns/op | B/op | Allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Existing `Unpack()` baseline | 345.7 | 592 | 3 | 1.00x |
| Reused `UnpackInto()` | 237.7 | 16 | 2 | 1.45x faster |

The remaining small allocation profile comes from decoding owned string/byte
values; the result slice allocation is removed. The API is opt-in and the
existing `Unpack` signature is unchanged.
