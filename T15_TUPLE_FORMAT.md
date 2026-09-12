# T-G15 Typed Positional Tuple Format

This feature adds an optional typed schema for positional packed tuples in
`hat/hatDataStructure`. It is additive: existing `NewPackedTuple` callers keep
the previous compact representation and behavior.

## Why

Untyped packed tuples are useful for very small, already-encoded records, but a
caller that needs defaults, NULL handling, generated fields, or fixed-width
numeric/date values otherwise has to duplicate validation and encoding rules.
`TupleFormat` centralizes those rules while retaining the existing contiguous
data plus offset-table layout.

## Usage

```go
format, err := hatDataStructure.NewTupleFormat(1, []hatDataStructure.TupleFieldSpec{
	{Name: "id", Type: hatDataStructure.TupleFieldInt64},
	{Name: "name", Type: hatDataStructure.TupleFieldString},
	{Name: "created_at", Type: hatDataStructure.TupleFieldTimestamp},
	{Name: "note", Type: hatDataStructure.TupleFieldString, Nullable: true},
})
if err != nil {
	return err
}

tuple, err := format.Pack([]hatDataStructure.TupleFieldValue{
	hatDataStructure.TupleInt64(42),
	hatDataStructure.TupleString("Ada"),
	hatDataStructure.TupleTimestamp(time.Unix(0, 0)),
	hatDataStructure.TupleNull(),
})
if err != nil {
	return err
}

if err := format.Validate(tuple); err != nil {
	return err
}
values, err := format.Unpack(tuple)
```

`Pack` accepts positional values. A missing field is filled from `Default`, or
from `Generated` when a generator is configured; a missing nullable field is
encoded as NULL. Explicit NULL is accepted only for nullable fields. Generators
receive the values resolved so far, in field order.

Supported field types are strings, byte slices, signed and unsigned 64-bit
integers, float64, bool, UTC dates, and UTC timestamps. Strings and bytes are
stored as raw field bytes. Integer and temporal fields use fixed-width
big-endian encoding; dates use Unix days and timestamps use Unix nanoseconds.

`Validate` checks the tuple shape and fixed-width/type invariants without
copying field data. `Unpack` returns independent byte slices for variable-width
values, so callers can safely retain or mutate the returned values.

## Memory and compatibility

The packed data remains contiguous and the offset table remains `uint32` based.
Typed tuples with at least one NULL carry a validity slice; tuples built by the
legacy constructors do not allocate that slice, preserving their previous
allocation behavior. `TupleFieldOffsetCache.FieldValid` reports legacy fields
as valid for backward compatibility.

This format is not enabled globally and does not replace the minimal legacy
constructor. Use it when schema-aware validation/defaults are worth the small
packing cost; use `NewPackedTuple` for already-encoded hot paths.

## Benchmark

Command:

```text
make benchmark-t-g15-tuple-format
```

Five runs on Linux/amd64, AMD Ryzen 9 5950X:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `TupleFormat.Pack` | 262.5-280.4 | 568 | 4 |
| `NewPackedTuple` baseline | 57.61-58.68 | 88 | 2 |

The typed path is approximately 4.6x slower and allocates 6.5x more bytes in
this small-record microbenchmark. That is an intentional capability tradeoff,
not a claim that typed packing beats the existing raw constructor. The
baseline path remains the default, and the typed path adds schema behavior that
the baseline does not provide.

Focused correctness, package, race, and vet checks pass through the T-G15
Makefile targets. The tests cover defaults, generated fields, NULL and
non-NULL validation, type/shape rejection, defensive copies, and date/time
round trips.
