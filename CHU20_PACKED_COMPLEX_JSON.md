# CH-U20 Packed Complex JSON Subcolumns

CH-U20 extends the existing typed JSON subcolumn contract to object and array
paths. It is opt-in: a resolver returns a `ColumnarBatch` through
`ColumnarJSONSubcolumnSourceResolver`, and `available=false` keeps the existing
row or map-subcolumn path.

## Representation

`MaterializeJSONSubcolumn` infers `ColumnarJSONSubcolumnJSON` for paths whose
non-null values are objects or arrays. The column stores:

- one immutable `JSONData []byte` buffer containing canonical JSON payloads;
- `JSONOffsets []uint32` with one boundary per row;
- the existing packed `Present` and `Validity` bitmaps.

Missing values have no payload. JSON `null` has presence without validity.
`Value` returns an immutable `json.RawMessage` view into the packed buffer.

## SQL Semantics

- `JSON_QUERY` decodes the selected raw JSON value and matches the ordinary
  row path's object/array result.
- `JSON_EXISTS` uses only the presence bitmap and does not decode the payload.
- `JSON_VALUE` returns the same scalar-only error as the ordinary evaluator.
- mixed scalar/complex paths, malformed JSON, invalid offsets, and bounded
  retention failures are rejected and fall back without retaining a bad column.

The exact resolver contract remains caller-owned. The representation is
in-memory only and does not alter backup, storage, or wire formats.

## Measurement

`make benchmark-chu20` uses 1,024 documents and five `-benchtime=200ms`
samples. The packed query was 1.35x faster than the same-run row control,
used 1.66x less query heap, and used 1.17x fewer allocations. The one-time
materialization measured 3.16 ms, 1.72 MB, and 32,784 allocations; retained
packed bytes were 53,166 for the fixture. The measured CPU saving amortizes
that materialization after roughly five repeated reads.

See [BENCHMARK.md](BENCHMARK.md#ch-u20-packed-complex-json-subcolumns) for
raw samples and [CH031_TYPED_JSON_SUBCOLUMNS.md](CH031_TYPED_JSON_SUBCOLUMNS.md)
for the complete representation and API context.
