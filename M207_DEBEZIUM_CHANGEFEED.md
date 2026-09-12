# Debezium Changefeed

`hatSql.NewDebeziumChangefeed` adapts an opt-in differential SQL subscription
to keyed Debezium-style row events. It emits `r` for the initial snapshot, then
`c`, `u`, and `d` for create, update, and delete transitions.

```go
feed, err := hatSql.NewDebeziumChangefeed(hatSql.DebeziumChangefeedOptions{
    KeyColumns: []string{"id"},
    Source: hatSql.DebeziumSource{
        Name: "orders",
        Connector: "hatrie-cache",
        Server: "orders-cache",
    },
})
if err != nil {
    return err
}

changes, err := feed.Apply(batch)
if err != nil {
    return err
}
```

Each change contains a separate key and payload. The payload uses the standard
`before`, `after`, `source`, `op`, and `ts_ms` fields. `id`, `revision`, and
`frontier` preserve the originating subscription position for a durable sink.

The adapter requires `KeyColumns` to identify one row. Duplicate keys and
non-unit differential multiplicities are rejected instead of guessing an
incorrect before/after image. Progress-only batches emit no data. Reset batches
replace the retained keyed state and emit the resulting transitions.

The adapter is caller-owned and serial: do not call `Apply` concurrently. It
retains one owned row image per key. Normal SQL subscriptions do not construct
this state or pay this conversion cost.

## Measurement

The initial implementation copied all retained rows for every small update
batch. For a 1,024-row state and eight updates, it measured:

| Path | Time | Allocated bytes | Allocs |
| --- | ---: | ---: | ---: |
| Full-state copy | 357-419 us/op | 425,5 KB/op | 2,291/op |
| Validate then mutate affected keys | 22-24 us/op | 23.1 KB/op | 220-221/op |
| Improvement | about 16x lower | about 18x lower | about 10x fewer |

The optimized implementation validates the complete batch before mutating the
retained map, so malformed batches leave the previous state unchanged.
