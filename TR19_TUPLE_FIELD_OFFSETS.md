# TR-19: Cached Tuple Field Offsets

Status: accepted and implemented.

## Idea

Tarantool-style tuple readers avoid rescanning a packed tuple from byte zero
for every field access. A tuple can keep a compact offset table beside its
packed bytes, so field `i` is a direct slice of `data[offsets[i]:offsets[i+1]]`.

This is useful for SQL rows, projections, and other immutable records that are
read repeatedly by field number. It is deliberately an opt-in data structure;
it does not change the existing wire or storage formats.

## API

The implementation is in `hat/hatDataStructure`:

```go
cache, err := hatDataStructure.NewPackedTuple([][]byte{
    []byte("alice"),
    []byte("sg"),
    []byte("active"),
})
if err != nil {
    // handle invalid input
}

name, err := cache.Field(0)       // borrowed view: "alice"
region, err := cache.Field(1)     // borrowed view: "sg"
status, err := cache.Field(2)     // borrowed view: "active"
_ = name
_ = region
_ = status
```

For already-packed data, use `NewTupleFieldOffsetCache(data, lengths)`. That
constructor borrows `data` and copies only the offset table. For a caller that
has separate fields, `NewPackedTuple` creates one contiguous data buffer and
copies the field bytes into it.

Available operations:

- `Field(index)` returns a borrowed field slice without allocating.
- `FieldInto(index, dst)` copies a field into caller-owned storage.
- `Offset(index)` returns the byte offset of a field.
- `Bytes()` returns the packed data view.
- `FieldCount()` returns the number of fields.
- `Clone()` copies the packed data and offsets into an independent cache.

## Correctness and limits

- Lengths must sum exactly to the packed data length.
- Field indexes are checked, including negative indexes.
- Field counts and lengths are bounded so offset arithmetic stays representable.
- `Field` and `Bytes` return borrowed views. The backing bytes must remain live
  and unchanged while the cache is used. Use `Clone` when ownership is needed.
- The cache is read-safe after construction, but callers must not mutate the
  backing bytes concurrently with reads.
- Replacing fields requires building a new packed tuple; this structure targets
  immutable or copy-on-write records.

## Benchmark

Command:

```text
make benchmark-tuple-field-offsets
```

Environment: Linux, amd64, AMD Ryzen 9 5950X. Each benchmark used five samples
with `-benchtime=200ms`. Access benchmarks use a 1,024-field one-byte tuple and
read field 768. Construction benchmarks build a 512-field tuple and compare
the packed representation with copying each field into a conventional
`[][]byte` value.

The table reports the median sample and the ratio versus the baseline.

| Workload | Cached tuple | Baseline | Improvement |
| --- | ---: | ---: | ---: |
| Field access | 4.556 ns/op, 0 B/op, 0 allocs/op | 206.0 ns/op, 0 B/op, 0 allocs/op (scan prior lengths) | 45.2x faster |
| Tuple construction | 3,637 ns/op, 14,592 B/op, 2 allocs/op | 12,438 ns/op, 25,856 B/op, 513 allocs/op (`[][]byte` copies) | 3.42x faster, 1.77x lower bytes, 256.5x fewer allocs |

Raw samples from the run:

```text
BenchmarkTupleFieldOffsetCacheAccess: 4.557 4.640 4.556 4.467 4.491 ns/op
BenchmarkTupleFieldScan:               206.0 207.3 206.3 205.6 205.6 ns/op
BenchmarkNewPackedTuple:             3637 3679 3548 3529 3724 ns/op, 14592 B/op, 2 allocs/op
BenchmarkNaiveTupleCopies:          12289 12438 12477 12531 12355 ns/op, 25856 B/op, 513 allocs/op
```

The access result is the main win: field lookup changes from O(field index) to
O(1). Construction also improves for this workload because one contiguous
buffer and one offset allocation replace per-field byte copies and slice
backing arrays.

The offset table costs 4 bytes per field plus the cache's fixed slice headers.
That memory is worthwhile for repeated random access, but a one-shot sequential
scan may not benefit enough to justify it. Existing callers can continue using
their current representation.

## Verification

Passed:

- `make test-tuple-field-offsets`
- `make test-tuple-field-offsets-package`
- `make race-tuple-field-offsets`
- `make vet-tuple-field-offsets`
- `make benchmark-tuple-field-offsets`
