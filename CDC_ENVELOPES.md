# CDC Envelopes

`hatSql.TypedTableChange` is the normalized in-process change envelope for a
typed table. It contains the application key, one of `INSERT`, `UPDATE`, or
`DELETE`, the relevant `Before` and `After` typed rows, and the table's
monotone `Sequence`.

`Upsert` emits `INSERT` for a new key and `UPDATE` for a replacement;
`Delete` emits `DELETE`. Consumers can apply the same envelope to joins,
aggregates, distinct arrangements, and other projections. Invalid row shapes
and failed mutations are rejected before a change is published, so consumers
never receive a partial mutation.

This normalizes the local CDC shape only. It does not assign external broker
offsets, source transaction IDs, or delivery acknowledgements.

## External Dynamic Envelopes

`hatSql.CDCEnvelope` is the importable boundary for a connector that has
already decoded a dynamic source event into a key and optional `Before` and
`After` `hatSql.Row` maps. `hatSql.NormalizeCDCEnvelope` returns a
`hatSql.CDCChange` with one canonical operation:

```go
change, err := hatSql.NormalizeCDCEnvelope(hatSql.CDCEnvelope{
	Sequence:  17,
	Operation: "u",
	Key:       " customer-7 ",
	Before:    hatSql.Row{"id": 7, "name": "old"},
	After:     hatSql.Row{"id": 7, "name": "new"},
})
if err != nil {
	return err
}
// change.Operation == hatSql.CDCOperationUpdate
// change.Key == "customer-7"
```

The accepted aliases are:

| Canonical operation | Accepted aliases | Required shape |
| --- | --- | --- |
| `INSERT` | `i`, `c`, `create`, `insert`, `r`, `read`, `snapshot` | non-nil `After`, nil `Before` |
| `UPDATE` | `u`, `update` | non-nil `Before` and `After` |
| `DELETE` | `d`, `delete`, `remove` | nil `After`; `Before` may be omitted for key-only deletes |
| shape-derived | `replace`, `upsert` | `After` only becomes `INSERT`; both rows become `UPDATE` |

Operation matching is case-insensitive and surrounding whitespace is ignored.
Keys are trimmed and empty keys are rejected. The sequence is copied into the
result unchanged. Invalid input returns an error matching
`hatSql.ErrCDCEnvelopeInvalid` before a change is returned.

For JSON input, use the standard-library decoder wrapper:

```go
change, err := hatSql.DecodeCDCEnvelopeJSON(payload)
```

It accepts `{"op":"c","key":"7","after":{"id":7}}` and
`{"operation":"r","key":"7","after":{"id":7}}`. JSON decoding creates
the row maps; normalization itself only validates and returns those maps. The
returned rows are borrowed and must not be mutated or retained past the
source-owned lifetime unless the caller makes a copy.

This boundary does not interpret Materialize differential records, assign
broker offsets, or infer transaction boundaries. A connector maps its source
metadata and any `diff` values before calling it. Tarantool tuple conversion
and composite-key encoding are also connector responsibilities.

The exact normalization benchmark and tradeoff are recorded in
[BENCHMARK.md](BENCHMARK.md#mz-015-cdc-envelope-normalization).

```sh
make test-mz015-cdc
make benchmark-mz015-cdc
```
