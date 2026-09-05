# SQL Index Rebuild Progress

`HatTrie.RunScheduledSQLJSONIndexRebuildsWithProgress` is an opt-in control
surface for queued JSON index maintenance. It reports transitions without
including source values or indexed rows:

```go
processed, err := trie.RunScheduledSQLJSONIndexRebuildsWithProgress(
	ctx,
	1,
	func(progress hatriecache.SQLJSONIndexRebuildProgress) {
		log.Printf("%s %s: %s (%d/%d)", progress.Key, progress.Field,
			progress.State, progress.Processed, progress.Total)
	},
)
```

The states are `queued`, `running`, `completed`, `failed`, and `canceled`.
Cancellation is cooperative between rebuild units. A canceled or failed unit
is put back at the front of the queue and can be retried by calling the method
again with a live context. A rebuild unit still builds and publishes its index
atomically; this API does not interrupt JSON decoding or resume inside one
large unit. Use `SQLJSONIndexMaintenanceStats` for the existing durable
counter view.

`limit <= 0` processes the currently queued work until the queue is empty; a
positive limit bounds the number of completed units. Passing a nil context
returns an error. Existing `RunScheduledSQLJSONIndexRebuilds` behavior and all
default index behavior remain unchanged.
