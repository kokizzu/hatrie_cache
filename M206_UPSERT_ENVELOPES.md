# M206 Upsert Envelopes

`hatSql.UpsertEnvelope` is an importable current-state boundary for keyed
change consumers. It carries one stable string key, the current `Row` image,
and optional source position metadata. A nil `Row` is a delete tombstone.

The API is opt-in. It does not change SQL execution, CDC delivery, or existing
JSON formats until a caller uses it.

## API

```go
change := hatSql.CDCChange{
	Sequence: 42,
	Operation: hatSql.CDCOperationUpdate,
	Key: "customer-7",
	After: hatSql.Row{"id": 7, "name": "current"},
}

envelope, err := change.AsUpsertEnvelope()
if err != nil {
	return err
}
// envelope.Key == "customer-7"
// envelope.Row["name"] == "current"
```

Supported adapters:

- `NormalizeUpsertEnvelope` trims and validates an already assembled value.
- `CDCChange.AsUpsertEnvelope` maps INSERT and UPDATE to the `After` image and
  DELETE to a tombstone.
- `DebeziumChange.AsUpsertEnvelope` maps create, update, and read payloads to
  the `After` image and delete payloads to a tombstone.
- `DecodeUpsertEnvelopeJSON` accepts the canonical `row` field plus the
  mutually exclusive `value` and `after` aliases.

Rows are borrowed. The adapters do not clone or modify row maps, so the caller
must retain ownership and avoid concurrent mutation.

## JSON

Current row:

```json
{"sequence":42,"key":"customer-7","row":{"id":7,"name":"current"}}
```

Delete tombstone:

```json
{"sequence":43,"key":"customer-7","row":null}
```

`value` and `after` are accepted as producer aliases, but supplying more than
one image field is rejected. Empty keys, invalid row shapes, unsupported
operations, and invalid before/after combinations return
`ErrUpsertEnvelopeInvalid`.

## Stable Debezium keys

`DebeziumChangefeed` computes the canonical key once while it emits each
change and stores it in `DebeziumChange.StableKey`. The field is an in-process
optimization and is omitted from Debezium JSON (`json:"-"`); the resulting
`UpsertEnvelope.Key` is the wire-facing stable key.

When a caller manually constructs a `DebeziumChange` without `StableKey`, the
adapter still works by deterministically serializing the structured `Key` map.
That fallback is correct but materially more expensive. Prefer changes emitted
by `NewDebeziumChangefeed`, or populate `StableKey` when a trusted producer
already has the canonical key.

## Measurement

Command:

```text
make m206-benchmark
```

The benchmark uses 256 events per operation on `linux/amd64`, AMD Ryzen 9
5950X, with five samples. The baseline is direct envelope assignment with
precomputed keys; it does not include validation.

| Path | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | --- |
| CDC direct assignment baseline | 570.3 | 0 | 0 | 1.00x |
| CDC `AsUpsertEnvelope` | 3,545 | 0 | 0 | 6.22x slower |
| Debezium direct assignment baseline | 139.7 | 0 | 0 | 1.00x |
| Debezium cached `StableKey` | 4,359 | 0 | 0 | 31.2x slower |
| Debezium map-key fallback | 133,410 | 36,946 | 1,536 | 955x slower |

The adapter is a correctness and common-boundary feature, not a replacement
for a trusted direct struct assignment in an ultra-hot internal loop. The
important optimization is the Debezium cached-key path: compared with the
fallback it is about 30.6x faster for this fixture and removes all measured
per-batch heap allocation. Key width and row shape change absolute values.
