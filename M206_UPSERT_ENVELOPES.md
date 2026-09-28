# M206 Upsert Envelopes

Materialize-style upsert streams need only a stable key and the current row
image. `hatSql.CDCEnvelope` deliberately keeps `before` and `after` images for
change-data-capture consumers; `hatSql.UpsertEnvelope` is the smaller contract
for consumers that maintain current state and do not need the previous image.

## API

```go
change, err := hatSql.NormalizeUpsertEnvelope(hatSql.UpsertEnvelope{
    Sequence: 42,
    Key:      "customer-42",
    Row:      hatSql.Row{"name": "Grace", "active": true},
})
if err != nil {
    return err
}
// change.Key is customer-42 and change.Row is the current image.
```

The canonical JSON form is:

```json
{"sequence":42,"key":"customer-42","row":{"name":"Grace","active":true}}
```

Deletes are explicit tombstones. The key remains present and `deleted` is true:

```json
{"sequence":43,"key":"customer-42","deleted":true,"row":null}
```

`NormalizeUpsertEnvelope` trims and requires a non-empty key, requires a row for
non-deleted changes, and rejects a row on a tombstone. Row maps are borrowed,
not copied, so callers must keep ownership rules clear. The API does not infer
ordering, deduplicate sequences, or persist data; those policies belong to the
source/consumer that owns them.

## Compatibility And Safety

This is additive and opt-in. Existing CDC envelopes, SQL commands, wire
formats, and persistence defaults are unchanged. Malformed JSON, invalid row
shapes, missing keys, and contradictory tombstones return
`ErrUpsertEnvelopeInvalid` without returning a partial change.

## Measurement

The benchmark compares the existing before/after CDC normalization with the
new current-image normalization using five samples in one clean worktree on an
AMD Ryzen 9 5950X. Values below are medians from the measured run; rerun the
target for the current machine.

| Workload | Existing CDC | Upsert | Result |
|---|---:|---:|---|
| In-memory normalize | 163.6 ns, 64 B, 1 alloc | 91.18 ns, 48 B, 1 alloc | 1.79x faster, 1.33x lower bytes |
| JSON decode | 6,783 ns, 1,248 B, 29 allocs | 5,100 ns, 984 B, 22 allocs | 1.33x faster, 1.27x lower bytes, 1.32x fewer allocs |

The improvement comes from avoiding the old image and CDC operation
canonicalization. The tradeoff is semantic: an upsert consumer cannot recover
the previous row from this envelope. Use `CDCEnvelope` when before/after audit
or diff data is required.

## Verification

```text
make test-m206-upsert-envelope
make race-m206-upsert-envelope
make benchmark-m206-upsert-envelope
```
