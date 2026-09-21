# M206 Upsert Envelopes

M206 adopts the Materialize-style keyed upsert boundary for consumers that
maintain a current table image. `hatSql.UpsertChangefeed` converts a
`QuerySubscriptionDeltaBatch` into `UpsertEnvelope` values with:

- `Key`: the declared stable key columns only;
- `Current`: the complete current row for an upsert;
- `Deleted`: a tombstone marker with a nil `Current` image; and
- `ID`, `Revision`, and `Frontier`: the source subscription position.

The adapter is importable from `hatSql`, stateful, caller-owned, and not safe
for concurrent `Apply` calls. Delivered key and row maps are detached, so a
consumer can mutate an envelope without corrupting the retained state.

## Example

```go
feed, err := hatSql.NewUpsertChangefeed(hatSql.UpsertChangefeedOptions{
    KeyColumns: []string{"id"},
})
if err != nil {
    return err
}

events, err := feed.Apply(batch)
if err != nil {
    return err
}
for _, event := range events {
    if event.Deleted {
        remove(event.Key)
        continue
    }
    put(event.Key, event.Current)
}
```

The first non-progress batch is emitted as a snapshot of positive rows.
Subsequent unit-multiplicity insert/retract pairs become one current-image
upsert. A retraction becomes a tombstone. A reset batch replaces the retained
state and emits changed/new rows followed by stable-key tombstones for rows
that disappeared.

## Validation

The adapter rejects missing or nil key fields, duplicate key columns, duplicate
source keys, non-unit multiplicity, and deletion of an unknown key. It does
not retain before images or differential counts; consumers that need those
semantics should use `DebeziumChangefeed` or the differential subscription
directly.

## Benchmark

Command:

```text
make benchmark-m206-upsert-envelope
```

Linux amd64, AMD Ryzen 9 5950X, five samples at two seconds per sample. The
workload applies one initial row and repeated update pairs to the same key.

| Adapter | Raw ns/op samples | Median ns/op | B/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| Debezium before/after baseline | 4034; 4145; 3990; 4097; 4163 | 4097 | 2726 | 28 |
| M206 upsert envelope | 3197; 3298; 3678; 3774; 3596 | 3596 | 2293 | 26 |

Relative to the baseline, M206 is about 1.14x faster, uses 1.19x less
allocated memory, and performs 1.08x fewer allocations. The gain comes from
omitting the before image and transferring the already-detached positive row
into retained state instead of cloning it again. The tradeoff is intentional:
M206 cannot answer before-image or arbitrary differential-multiplicity
questions.
