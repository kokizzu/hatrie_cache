# T227 Per-Field Schema Validation

T227 closes the gap between a declared `hatSchema.Column` and the values
accepted by `hatSchema.ValidateRows`. The new exported
`hatSchema.ValidateFieldValue` helper validates one value independently, and
`ValidateRows` applies it to every declared column before evaluating existing
`NOT NULL`, `CHECK`, `UNIQUE`, and foreign-key rules.

## Contract

- `nil` is accepted only for nullable columns; `NotNull` columns reject it.
- Integer columns accept signed and unsigned integer Go kinds, including
  aliases; number columns additionally accept finite floating-point values.
- Text, binary, boolean, temporal, UUID/IP, duration, decimal, and JSON fields
  reject incompatible Go value shapes.
- `ENUM8` and `ENUM16` require a string from the declared `EnumValues` set.
- Validation is read-only and does not mutate the schema or row map.
- Existing table-level constraints still run after field validation, preserving
  their previous ordering and semantics.

Example:

```go
column := hatSchema.Column{Name: "age", Type: hatSchema.TypeInteger}
if err := hatSchema.ValidateFieldValue(column, int64(42)); err != nil {
	return err
}
```

## Measurement

Five `-benchmem` samples on Linux/amd64 with an AMD Ryzen 9 5950X measured one
valid three-field row through `ValidateRows`:

| Workload | Median CPU | Memory | Relative CPU |
| --- | ---: | ---: | ---: |
| Before field validation | 194.1 ns/op | 0 B/op, 0 allocs/op | 1.00x |
| After field validation | 214.2 ns/op | 0 B/op, 0 allocs/op | 1.10x |

The feature adds 20.1 ns/op, or about 10.4% CPU, with no allocation increase.
That cost is paid at the explicit schema validation boundary; it prevents
malformed values from reaching constraint evaluation or publication. The
benchmark is reproducible with `make benchmark-t227`.

## Verification

```text
make test-t227
make test-c154-schema-package
make test-ch004-schema
make test-schema-materialized
make verify-t227
```

The focused test covers valid scalar values, nullable `nil`, `NOT NULL`, wrong
scalar types, enum membership, temporal values, binary values, and JSON rows.
