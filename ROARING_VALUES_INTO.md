# RoaringBitmap `ValuesInto`

`RoaringBitmap.ValuesInto` enumerates all bitmap values in ascending order into
caller-owned storage. It is the reusable counterpart to `Values()` for code
that repeatedly scans the same bitmap.

```go
values := make([]uint32, 0, int(bitmap.Count()))
for range requests {
    values = bitmap.ValuesInto(values)
    consume(values)
}
```

The method clears the destination, grows it only when its capacity is too
small, and appends through the existing container enumerators. It does not
expose the bitmap's internal container storage. `Values()` remains unchanged
for callers that want a newly allocated result.

## Measurement

Workload: a bitmap containing 100,000 sequential values, measured with
`go test -benchmem -count=10` on an AMD Ryzen 9 5950X.

| Operation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `Values()` before the change | 165,238 | 401,408 | 1 |
| `Values()` in the comparison run | 165,906 | 401,408 | 1 |
| `ValuesInto()` with reused buffer | 152,281 | 0 | 0 |

The reusable path was about **1.09x faster** than the same-run `Values()`
comparison and removed the result allocation. The small difference between
the two `Values()` runs is normal benchmark variance; its implementation and
semantics were not changed. The benchmark resets its timer after preallocating
the reusable destination, so the zero `B/op` result measures repeated calls,
not one-time setup. The first `ValuesInto` call still grows the destination
when needed, so callers should retain the returned slice.
