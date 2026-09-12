# Changefeed Progress

`hatReplication.ChangefeedFrontier` is a small progress-watermark contract for
changefeed producers and consumers. It is inspired by Materialize
`SUBSCRIBE ... WITH (PROGRESS)`: a progress message means that the producer has
observed every update through the reported sequence, so a consumer can persist
that watermark after it has durably applied the preceding updates.

## Producer

The frontier is safe for concurrent producers. Sequence zero is a valid empty
frontier. Equal advances are idempotent, while a lower sequence is rejected so
a stale producer cannot move a published watermark backward.

```go
frontier := hatReplication.NewChangefeedFrontier(lastCheckpoint)

progress, err := frontier.Advance(batchLastSequence)
if err != nil {
	return err
}
if progress.Progressed {
	return checkpoint(progress.Sequence)
}
```

`Progress()` returns the current progress message without advancing the
frontier. `Current()` is a cheap scalar read and returns zero for a nil
frontier. Nil `Progress` and `Advance` calls return
`ErrChangefeedFrontierNil`; regressions return
`ErrChangefeedFrontierRegressed` together with the current watermark.

The type deliberately tracks a sequence, not wall-clock time. Callers can map
the sequence to a journal LSN, partition offset, or logical timestamp. The
frontier does not itself store events, write checkpoints, or provide a network
subscription; those responsibilities remain with the producer and durable
consumer.

## Consumer Rule

Apply all updates in a batch before persisting its progress message. On
reconnect, resume from the persisted sequence and reject a producer that sends
a regressed progress message. For multi-partition streams, combine per-stream
frontiers only after the application has a defined policy for whether a global
minimum or a per-partition vector is required; this type intentionally does not
pretend that one scalar orders independent partitions.

## Benchmark

Run with `make benchmark-m201`. Five runs on an AMD Ryzen 9 5950X:

| Operation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Progress | 0.4772 | 0 | 0 |
| Advance | 2.339 | 0 | 0 |

This is an additive API and does not change existing replication, replay, or
read-consistency defaults. There is no previous implementation baseline; the
measured cost is the new producer/consumer watermark operation itself.
