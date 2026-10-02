# T-U19 Durable Tuple Field-Update Journal

This adds an opt-in TFJ1 binary record for the existing atomic
`TupleFieldUpdate` operations. A record contains an opaque row key, a schema
version, a strictly ordered sequence, the update batch, and a CRC32C checksum.
It is intended for a caller-owned durable journal or WAL; this package does
not open files, choose fsync policy, or route keys to storage.

## Replay Contract

- `MarshalTupleFieldUpdateJournal` rejects zero versions, duplicate fields,
  unsupported operations, oversized values, and records over 1 MiB before
  allocating the encoded buffer.
- `UnmarshalTupleFieldUpdateJournal` validates magic, wire version, every
  length, operation kind, bounds, and CRC32C before returning owned slices.
- `TupleFieldUpdateJournalApplier` accepts a configured checkpoint sequence,
  rejects duplicates/out-of-order records, rejects gaps after initialization,
  and advances only after the tuple update succeeds.
- Schema version mismatch, overflow, type errors, and malformed operations do
  not advance the applier and do not mutate the source tuple.
- The caller owns durable append, fsync, crash recovery, key routing, and
  snapshot/checkpoint persistence.

## Example

```go
record := hatDataStructure.TupleFieldUpdateJournalRecord{
    Sequence:      42,
    SchemaVersion: format.Version(),
    Key:           []byte("account:42"),
    Updates: []hatDataStructure.TupleFieldUpdate{
        {Index: 0, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 1},
    },
}
wire, err := hatDataStructure.MarshalTupleFieldUpdateJournal(record)
if err != nil {
    return err
}

decoded, err := hatDataStructure.UnmarshalTupleFieldUpdateJournal(wire)
if err != nil {
    return err
}
updated, err := applier.Apply(tuple, format, decoded)
if err != nil {
    return err
}
_ = updated
```

The record's key is intentionally opaque. A storage owner should authenticate
the journal/file boundary separately when required and should not treat CRC32C
as a cryptographic authenticity check.

## Measured Tradeoff

Commands: `make round26-baseline-benchmark` and
`make round26-adoption-benchmark`, each using
`-benchtime=300ms -count=5 -benchmem`.

Machine: Linux/amd64, AMD Ryzen 9 5950X 16-Core Processor.

| Workload | Samples (ns/op) | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Baseline direct `ApplyUpdates` | 175.0, 193.3, 182.0, 177.1, 171.9 | 177.1 | 40 | 2 |
| Adoption direct `ApplyUpdates` | 176.1, 173.4, 175.2, 174.8, 175.6 | 175.2 | 40 | 2 |
| TFJ1 marshal/decode/apply | 676.3, 690.2, 691.5, 683.3, 695.3 | 690.2 | 288 | 6 |

The durable path is about 3.90x the baseline CPU time, uses 7.2x the measured
temporary bytes, and uses three times as many allocations for this two-field
record. The direct update path remains allocation and CPU equivalent to the
baseline. The journal is therefore deliberately opt-in for crash recovery or
replication, not an automatic replacement for in-memory updates.

## Verification

- `make round26-adoption-focused`
- `make round26-adoption-test`
- `make round26-adoption-race-focused`
- `make round26-adoption-vet`
- `make round26-baseline-benchmark`
- `make round26-adoption-benchmark`

