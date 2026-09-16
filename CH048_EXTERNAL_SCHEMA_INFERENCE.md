# CH-048 External Schema Inference

`hat/hatSql` now exposes bounded schema inference for dynamic external rows,
JSON documents, and newline-delimited JSON. It is an opt-in planning helper:
existing CSV, JSON, and external-table import paths keep their current
behavior and do not pay inference overhead unless a caller invokes the new
API.

## API

```go
columns, err := hatSql.InferExternalJSONSchema(data, hatSql.ExternalSchemaInferenceOptions{
	MaxRows:               10_000,
	MaxColumns:            256,
	MaxBytes:              8 << 20,
	AllowNumericPromotion: false,
})
if err != nil {
	return err
}
```

The available entry points are:

- `InferExternalSchema(rows, options)` for already-decoded `[]Row` values.
- `InferExternalJSONSchema(data, options)` for one object or an array of
  objects.
- `InferExternalNDJSONSchema(data, options)` for one object per non-empty
  line.
- `tables.InferSchema(name, options)` for a registered `ExternalTable`.

The result is a stable, lexicographically sorted `[]SQLRowBinaryColumn`.
Missing fields and explicit `null` values make a column nullable. An empty
input, malformed JSON, non-object JSON values, trailing JSON values, or a
configured bound violation returns an error instead of silently inferring a
partial schema.

## Defaults And Safety

Zero-valued options use these bounded defaults:

| Option | Default | Maximum |
| --- | ---: | ---: |
| `MaxRows` | 4,096 | 1,000,000 |
| `MaxColumns` | 1,024 | 65,536 |
| `MaxBytes` | 64 MiB | 64 MiB |

Negative values and values above the maximum are rejected. Bounds are
deliberately finite because schema inference reads and retains the sampled
rows. Callers should choose limits appropriate for their input instead of
sampling unbounded user-controlled data.

## Type Rules

The inference result uses existing RowBinary-compatible types:

| Values observed | Inferred type |
| --- | --- |
| `bool` | `SQLRowBinaryBool` |
| Signed integers | `SQLRowBinaryInt64` |
| Unsigned integers | `SQLRowBinaryUint64` |
| Floating-point values | `SQLRowBinaryFloat64` |
| `string` | `SQLRowBinaryString` |
| `[]byte` | `SQLRowBinaryBytes` |
| `time.Time` | `SQLRowBinaryDateTime` |
| `time.Duration` | `SQLRowBinaryDuration` |
| Nested values or unsupported values | `SQLRowBinaryJSON` |

JSON decoding uses `json.Decoder.UseNumber`, so integers such as
`9007199254740993` remain integers instead of being rounded through
`float64`. A conflicting type, such as a number and a string in the same
column, conservatively becomes `SQLRowBinaryJSON`.

`AllowNumericPromotion` is false by default. When enabled, mixed signed,
unsigned, and floating numeric values may become `SQLRowBinaryFloat64`. This
is convenient for heterogeneous feeds but can lose integer precision for
values that cannot be represented exactly by `float64`; keep it disabled when
exact integer semantics matter.

CSV import remains header-driven and text-valued. Callers that need typed CSV
columns can decode rows themselves and pass them to `InferExternalSchema`.

## Example

```go
data := []byte(`[{"id":9007199254740993,"name":"one"},{"id":9007199254740994}]`)
columns, err := hatSql.InferExternalJSONSchema(data, hatSql.ExternalSchemaInferenceOptions{})
// columns:
// [{Name: "id", Type: SQLRowBinaryInt64, Nullable: false},
//  {Name: "name", Type: SQLRowBinaryString, Nullable: true}]
```

Inference does not register a table, alter a schema, or import rows by itself.
Use the returned columns to construct the typed destination or to validate a
planned external query before doing the actual import.

## Measured Tradeoff

The benchmark uses 256 JSON rows on an AMD Ryzen 9 5950X Linux/amd64 host,
with `-benchmem -benchtime=200ms -count=5`. The existing path parses rows with
`ParseJSONRows`; the new JSON path parses with `UseNumber` and infers the
schema. See the raw samples and method notes in [BENCHMARK.md](BENCHMARK.md#ch-048-external-schema-inference).

The JSON convenience path measured `1.07x` the baseline CPU, `+4.6%` bytes per
operation, and `+5.3%` allocations. Inference over already-decoded rows took
about `44.6 us`, `368 B/op`, and `2 allocs/op` in the same run. Since the API
is explicit and the existing import paths are unchanged, this cost is paid
only by callers that request inference.
