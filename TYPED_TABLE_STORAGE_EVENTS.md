# Typed-table Storage Events

Typed-table storage events are an opt-in, bounded lifecycle log inspired by
ClickHouse part and merge logs. They make deferred logical-delete work
observable without retaining keys, SQL text, or row values.

## Enable It

```go
table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
	Name: "orders",
	Columns: []hatSql.TypedTableColumn{
		{Name: "amount", Kind: hatSql.TypedTableInt64},
	},
	PatchParts: hatSql.TypedTablePatchOptions{
		Enabled:        true,
		MergeThreshold: 1024,
	},
	StorageEvents: hatSql.TypedTableStorageEventLogOptions{
		Enabled:  true,
		Capacity: 256,
	},
})
if err != nil {
	return err
}

events, enabled := table.StorageEvents(100)
if enabled {
	for _, event := range events {
		fmt.Printf("%s rows=%d->%d pending=%d\n",
			event.Kind,
			event.PhysicalRowsBefore,
			event.PhysicalRowsAfter,
			event.PendingDeletes,
		)
	}
}
```

`Enabled` defaults to false. When enabled, `Capacity <= 0` selects the sane
default of 256 retained events. A larger capacity is allowed when a longer
diagnostic window is needed. The ring is allocated once when the table is
created; events beyond the capacity evict the oldest event.

## Event Kinds

| Kind | When it is emitted | Meaning |
| --- | --- | --- |
| `base_part_created` | The first physical row is inserted into an empty table | The table moved from zero physical rows to its first base storage. |
| `patch_part_created` | The first logical delete after the pending-delete count reaches zero | A new deferred-delete patch batch began. Additional deletes in that batch do not create more events. |
| `patch_part_merged` | `CompactPatchParts` or threshold-triggered background compaction removes tombstones | The patch batch was folded into the compact row arrays. `Duration` records the merge duration. |

Events are returned oldest-first. `StorageEvents(0)` or a negative limit
returns all retained events; a positive limit returns only the newest events.
The returned slice is independent from the table's ring and can be read while
other goroutines continue to mutate the table.

`TableSequence` identifies the table changefeed sequence associated with the
event. `Sequence` is the independent event-log sequence. `PhysicalRowsBefore`
and `PhysicalRowsAfter` count internal rows, including tombstoned rows before a
merge. `PendingDeletes` is the tombstone count after the event, and
`DeletedRows` is the number of rows represented by the patch or merge.

## Cost And Limits

The default-disabled path keeps a nil event-log pointer and does not call the
clock, allocate event records, or change the existing delete/upsert allocation
profile. With the default 256-slot ring enabled, the measured workload kept
the same `1056 B/op` and `4 allocs/op`; the opt-in timestamp and ring writes
added about 3% CPU in the five-sample benchmark. The ring's retained memory
is bounded by `Capacity` and is reclaimed with the table.

This is an in-memory diagnostic history, not a durable WAL or backup format.
Events are reset when the table is rebuilt or restored, and they intentionally
exclude keys and values. Use the normal snapshot, journal, and typed-table
changefeed facilities for recovery or replication.
