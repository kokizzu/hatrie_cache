# M243 Arrangement Memory Metrics

M243 adopts a Materialize-inspired arrangement memory breakdown for the
existing `TypedTableAggregateArrangementStats` report. It does not change the
arrangement representation or add accounting work to writes.

## API

`TypedTableAggregateArrangement.Stats()` and
`TypedTableAggregateArrangements.Stats()` now expose:

| Field | Meaning |
| --- | --- |
| `EstimatedKeyBytes` | Estimated hash-bucket, ordering-reference, dictionary-metadata, and encoded group-key storage. |
| `EstimatedValueBytes` | Estimated retained typed group-value storage. |
| `EstimatedTraceBytes` | Estimated multiplicity-map storage for `MIN`, `MAX`, and `DISTINCT`, which preserves exact delete/update behavior. |
| `EstimatedBytes` | The legacy total; it remains exactly the sum of the three components. |

These are bounded accounting estimates, not runtime allocator or process RSS
readings. The table changelog is intentionally not included in
`EstimatedTraceBytes`; its retention is reported separately by the table
frontier fields already present in the stats response.

```go
stats, err := arrangement.Stats()
if err != nil {
    return err
}
if stats.EstimatedKeyBytes+stats.EstimatedValueBytes+stats.EstimatedTraceBytes != stats.EstimatedBytes {
    return errors.New("arrangement memory accounting mismatch")
}
```

The fields are always available in the structured stats JSON. The change is
diagnostic only: callers do not need to enable a flag, and all existing
arrangement updates, snapshots, checkpoints, and recovery paths are unchanged.

## Measurement

The focused benchmark calls `Stats()` on a 256-row, 32-group aggregate with
`SUM`, `MIN`, `MAX`, and `DISTINCT` state. Five `-benchmem` samples were taken
on Linux amd64, AMD Ryzen 9 5950X.

| Version | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Before M243 | 1,218 | 0 | 0 |
| After M243 | 1,200 | 0 | 0 |

Raw samples:

```text
before: 1279 1232 1183 1203 1218 ns/op; 0 B/op; 0 allocs/op
after:  1191 1341 1139 1207 1200 ns/op; 0 B/op; 0 allocs/op
```

The small median difference is within normal benchmark noise; no speedup is
claimed. The measured CPU, allocation, and retained-memory profile is
unchanged. The tradeoff is the larger monitoring JSON payload and the extra
component arithmetic when callers request arrangement stats.

## Verification

The regression test verifies nonzero key/value/trace components, exact total
preservation, and JSON field names. Run:

```text
make test-m243
make benchmark-m243
make race-m243
make vet-m243
```
