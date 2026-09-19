# CH-U25 Query Spill Quota

`hat/hatSpill` provides an importable, opt-in byte budget for temporary query
spill files and other bounded scratch storage.

```go
budget := hatSpill.NewBudget(512 << 20)
lease, err := budget.Reserve(ctx, chunkBytes)
if err != nil {
	return err
}
defer lease.Release()
```

Reservations are FIFO, reject requests larger than a finite limit, remove
canceled waiters, and never exceed the configured byte count. `TryReserve`
supports non-blocking execution. `Close` rejects new work and unblocks queued
waiters. Copies of a `Reservation` share an idempotent release token.

A limit of zero means unlimited accounting and is useful as a compatibility
default. The caller still owns temporary-file deletion and must release every
active reservation; this package does not claim durable recovery after a
process crash. `Snapshot` exposes bounded usage and queue diagnostics.

The quota path is intentionally opt in. The measured uncontended reservation
cost is about 19.74 ns/op, 4 B/op, and one allocation on the benchmark host;
the raw atomic counter baseline is about 3.57 ns/op with no allocation. A
snapshot costs about 22.03 ns/op with no allocation.
