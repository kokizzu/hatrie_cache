# Upsert Sources

`hatSql.TypedTable.Upsert` models an upsert source with one current row per
application key. A first write emits `INSERT`; a write for an existing key
replaces the stored row and emits `UPDATE` with both `Before` and `After`
values. `Delete` emits the corresponding `DELETE` change.

The change sequence is monotone per table and is carried on
`TypedTableChange`. Consumers can apply the envelope to arrangements and
projections without rescanning the source. A failed or invalid mutation does
not publish a partial row or changefeed entry.

This is an in-process typed-table source contract. It does not claim to
implement Kafka offsets, source transaction IDs, or durable exactly-once
delivery; those remain separate open items.
