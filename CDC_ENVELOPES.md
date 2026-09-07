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
