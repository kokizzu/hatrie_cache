# TR-20 Versioned Tuple Boundaries

Status: implemented and opt-in.

## Why

`TupleFormat` already validates field count, nullability, and physical field
encodings, but a `TupleFieldOffsetCache` did not carry the format version that
produced it. A record from an older or unrelated schema could therefore reach
a boundary where the caller only checked physical shape.

TR-20 adds a small `VersionedTuple` envelope. It records the `TupleFormat`
version, validates that version before validating the tuple, and can be
encoded as a bounded `HTV1` wire envelope for storage or peer transfer.

## Usage

```go
format, err := hatDataStructure.NewTupleFormat(7, []hatDataStructure.TupleFieldSpec{
    {Name: "id", Type: hatDataStructure.TupleFieldInt64},
    {Name: "name", Type: hatDataStructure.TupleFieldString},
})
if err != nil {
    return err
}

record, err := format.PackVersioned([]hatDataStructure.TupleFieldValue{
    hatDataStructure.TupleInt64(42),
    hatDataStructure.TupleString("orders"),
})
if err != nil {
    return err
}
if err := record.Validate(format); err != nil {
    return err
}

wire, err := hatDataStructure.MarshalVersionedTuple(record)
if err != nil {
    return err
}
restored, err := hatDataStructure.UnmarshalVersionedTuple(wire)
if err != nil {
    return err
}
return restored.Validate(format)
```

Existing callers can wrap an already-packed cache with
`NewVersionedTupleFromCache`. `VersionedTuple.ApplyUpdates` retains the version
and validates the updated bytes against the same format. `TupleFormat.Validate`
and the existing unversioned constructors remain compatible for callers that
do not need a schema fence.

## Wire format

The strict `HTV1` envelope contains:

1. Four-byte magic `HTV1`.
2. One wire-format version byte.
3. Schema version as an unsigned varint.
4. Field count as an unsigned varint.
5. For each field, an unsigned varint containing byte length plus one, followed
   by field bytes. Zero represents SQL `NULL`, while one represents an empty
   non-NULL field.

The decoder rejects unsupported versions, truncated varints, truncated fields,
trailing bytes, more than `1 << 20` fields, and payloads larger than 64 MiB
before accepting the tuple. Decoding only establishes a safe physical tuple;
callers must still call `VersionedTuple.Validate` with the expected format to
check the schema shape.

## Cost and compatibility

- `TupleFieldOffsetCache` is unchanged, so legacy records pay no per-record
  memory or CPU cost.
- `VersionedTuple` adds one `uint64` version field to the opt-in wrapper; it
  does not copy the tuple data.
- Version validation adds no allocation and was within benchmark noise after
  avoiding a duplicate format-definition check.
- Wire marshal/unmarshal are explicit boundary operations and allocate owned
  bytes. They are not silently inserted into the existing hot path.

## Benchmark

Linux/amd64, AMD Ryzen 9 5950X, five samples per benchmark. The reported
values are medians from the final run; `x` is versioned divided by the
unversioned baseline.

| Operation | Baseline | Versioned | Relative | Memory |
| --- | ---: | ---: | ---: | ---: |
| Validate typed tuple | 46.17 ns/op | 47.61 ns/op | 1.03x | 0 B/op, 0 allocs/op |
| Marshal HTV1 envelope | no previous equivalent | 115.1 ns/op | n/a | 80 B/op, 1 alloc/op |
| Unmarshal HTV1 envelope | no previous equivalent | 132.3 ns/op | n/a | 72 B/op, 4 allocs/op |

The validation cost is a small safety overhead. Because the envelope is opt-in,
existing workloads that do not need versioned storage or transfer are
unchanged.
