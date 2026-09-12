# T-G14 Tuple Field Operation Updates

## What changed

`TupleFieldOffsetCache.ApplyUpdates` applies one atomic positional operation per
field and returns a new packed tuple. It keeps the original cache unchanged,
validates the complete batch before allocating, and reuses the source offset
table when field widths do not change.

Supported operations:

- `TupleFieldSet` replaces a field with `Value`.
- `TupleFieldSplice` replaces `Remove` bytes at `Start` with `Insert`.
- `TupleFieldAddInt64` adds `Delta` to an exactly eight-byte big-endian signed
  `int64` field.

Duplicate field indexes are rejected so a batch has deterministic one-operation
semantics. Callers that need sequential operations on the same field can issue
separate batches and choose when to publish each returned cache.

## Example

```go
count := make([]byte, 8)
binary.BigEndian.PutUint64(count, 41)
cache, err := hatDataStructure.NewPackedTuple([][]byte{
    []byte("west"),
    count,
})
if err != nil {
    return err
}

updated, err := cache.ApplyUpdates([]hatDataStructure.TupleFieldUpdate{
    {Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("east")},
    {Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 1},
})
if err != nil {
    return err
}
```

For byte-oriented fields:

```go
updated, err := cache.ApplyUpdates([]hatDataStructure.TupleFieldUpdate{
    {
        Index: 0,
        Kind:  hatDataStructure.TupleFieldSplice,
        Start: 2,
        Remove: 2,
        Insert: []byte("XY"),
    },
})
```

## Semantics and safety

- The method is copy-on-write at the tuple level: the source data and offsets
  are never changed.
- Invalid indexes, duplicate indexes, unknown kinds, invalid splice ranges,
  incompatible `AddInt64` widths, and signed overflow return errors before the
  source is changed.
- Fixed-width set, splice, and add batches copy the packed data once and apply
  validated edits into that new buffer. The immutable offset table is shared
  safely when every field keeps its width.
- A length-changing batch allocates a new offset table and records the new
  boundaries.
- `TupleFieldAddInt64` is intentionally explicit about its representation; a
  raw tuple has no schema from which to infer numeric encoding.

## Benchmark

The benchmark ran on Linux, `amd64`, AMD Ryzen 9 5950X, with 512 eight-byte
fields and five 200 ms samples per case.

| Case | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `ApplyUpdates` fixed-width fast path | 3,607-3,768 | 4,096 | 1 |
| Naive field copy plus `NewPackedTuple` repack | 4,449-5,031 | 19,976 | 4 |

The update path is about 1.27x faster, uses about 4.9x less allocated memory,
and performs 4x fewer allocations on this workload. The benchmark targets the
fixed-width path; correctness tests also exercise length-changing splice and
overflow handling.

## Verification

```text
make test-t-g14-tuple-updates
make race-t-g14-tuple-updates
make test-t-g14-package
make vet-t-g14-tuple-updates
make benchmark-t-g14-tuple-updates
```

All listed targets pass.
