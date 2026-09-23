# T228 Tuple Format Compatible Readers

`TupleFormat` remains a strict positional schema. `NegotiateTupleFormat` still
selects only an exact common version and physical shape. T228 adds an explicit
`TupleFormatReader` for rolling schema changes after the caller has identified
the source and target formats.

## Example

```go
source, err := hatDataStructure.NewTupleFormat(1, []hatDataStructure.TupleFieldSpec{
	{Name: "id", Type: hatDataStructure.TupleFieldUint64},
	{Name: "name", Type: hatDataStructure.TupleFieldString},
})
if err != nil {
	return err
}
regionDefault := hatDataStructure.TupleString("sg")
target, err := hatDataStructure.NewTupleFormat(2, []hatDataStructure.TupleFieldSpec{
	{Name: "id", Type: hatDataStructure.TupleFieldUint64},
	{Name: "name", Type: hatDataStructure.TupleFieldString},
	{Name: "region", Type: hatDataStructure.TupleFieldString,
		Default: &regionDefault},
	{Name: "active", Type: hatDataStructure.TupleFieldBool, Nullable: true},
})
if err != nil {
	return err
}

reader, err := target.ReaderFor(source)
if err != nil {
	return err
}
legacyTuple, err := source.Pack([]hatDataStructure.TupleFieldValue{
	hatDataStructure.TupleUint64(42),
	hatDataStructure.TupleString("orders"),
})
if err != nil {
	return err
}
values, err := reader.Unpack(legacyTuple)
// values contains id, name, region="sg", and active=NULL.
```

The constructor spelling `NewTupleFormatReader(source, target)` is equivalent
to `target.ReaderFor(source)`. For an envelope that carries a source version,
use `reader.UnpackVersioned(versionedTuple)`; it rejects a version other than
the source version recorded when the reader was created.

## Compatibility Rules

- Common fields are positional and must retain the same name and physical
  `TupleFieldType`.
- Reordering, renaming, and type changes are rejected at reader construction.
- A source nullable field cannot be read by a target required field. A target
  may become more nullable because every source value remains valid.
- A source may contain additional trailing fields. The reader validates the
  complete source tuple, then returns only the target prefix.
- A target may add trailing fields only when each missing field has a declared
  default, generator, or nullable flag. Generators run in target field order
  and see the already decoded/resolved prefix.
- A missing required target field without one of those rules is rejected.
- The tuple field count must exactly match the source format. This prevents a
  reader from silently accepting a tuple from an unrelated schema.

This is deliberately prefix-only. The tuple bytes do not carry field names or
type metadata, so arbitrary field remapping cannot be inferred safely. Build a
reader from the registered source and target definitions after capability
negotiation or another authenticated schema registry decision.

## Cost

Command: `make benchmark-t228` (`-benchmem -count=5 -cpu=1`). Values below are
medians from the same benchmark invocation on Linux/amd64, AMD Ryzen 9 5950X.

| Workload | Median CPU | Memory | Result |
| --- | ---: | ---: | --- |
| Existing exact `TupleFormat.Unpack` control | 308.2 ns/op | 456 B/op, 3 allocs/op | 1.00x |
| Exact-shape `TupleFormatReader` | 309.6 ns/op | 456 B/op, 3 allocs/op | 1.005x CPU, no allocation change |
| Two-field legacy tuple to four-field target | 244.3 ns/op | 456 B/op, 2 allocs/op | migration path; not an apples-to-apples speed claim |

The exact-shape reader uses the existing unpack fast path, so the compatibility
adapter has effectively neutral CPU and memory cost for unchanged schemas. The
legacy path allocates one fewer object in this workload because it validates and
decodes only the two physical source fields while resolving the target suffix.
A separate pre-implementation baseline-only run measured 335.2 ns/op; repeated
benchmark variance is why the paired control above is used for the ratio.

## Verification

```text
make test-t228
make benchmark-t228
make race-t228
make vet-t228
```

Focused tests cover backward and forward additive reads, default and generated
fields, nullable widening, exact source counts, malformed physical fields,
unsafe schema changes, and version mismatch handling.
