# M228: Exactly-Once Source Restart

## Purpose

`ChangefeedExactlyOnceConsumer` keeps the last committed source offset and one
pending batch. A restart resumes at the first offset after the committed batch;
an uncommitted batch is returned as `Resume` so the caller can replay it.

The state machine has three decisions:

| Decision | Meaning |
| --- | --- |
| `Apply` | The contiguous batch is new and may be applied. |
| `Resume` | The same pending batch was delivered again after a restart. |
| `Skip` | The complete batch is already at or before the committed offset. |

`BeginBatch` rejects gaps, partial overlaps, concurrent different pending
batches, and invalid ranges. `CommitBatch` advances the source offset and
frontier together. Repeating the last committed batch ID is idempotent.

## Durable usage

```go
consumer, err := hatReplication.NewChangefeedExactlyOnceConsumer("orders")
if err != nil {
    return err
}

decision, err := consumer.BeginBatch(batch.ID, batch.StartOffset, batch.EndOffset)
if err != nil {
    return err
}
if decision.Action != hatReplication.ChangefeedExactlyOnceSkip {
    // Apply or resume the batch in the same durable transaction as the state.
    if err := applyBatch(batch); err != nil {
        return err
    }
    if _, err := consumer.CommitBatch(batch.ID, sourceFrontier); err != nil {
        return err
    }
}
snapshot, err := consumer.MarshalBinary()
```

Persist the snapshot atomically with the applied data, or make the destination
write idempotent. The library cannot make an arbitrary external side effect
exactly-once by itself. Persisting the metadata after an unrelated side effect
can still duplicate that side effect after a crash.

`MarshalBinary` and `UnmarshalBinary` use a bounded binary format with strict
version, length, flag, UTF-8, and trailing-byte validation. JSON remains useful
for human-readable diagnostics and compatibility, but is not the default
storage representation for this state.

## Measurements

Five benchmark samples, Linux amd64, Go test benchmarks, AMD Ryzen 9 5950X:

| Operation | Binary median | JSON median | Improvement | Allocation change |
| --- | ---: | ---: | ---: | ---: |
| Marshal | 192.4 ns/op | 653.0 ns/op | 3.39x faster | 112 vs 320 B/op; 1 vs 2 allocs |
| Unmarshal | 191.8 ns/op | 2,808 ns/op | 14.64x faster | 96 vs 424 B/op; 4 vs 11 allocs |
| Snapshot size | 97 B | 254 B | 2.62x smaller | n/a |

These results are workload-specific; run the repository benchmark target on the
deployment CPU before using them as capacity assumptions.

## Verification

```text
make test-m228-exactly-once-restart
make size-m228-exactly-once-restart
make benchmark-m228-exactly-once-restart
make race-m228-exactly-once-restart
make vet-m228-exactly-once-restart
```
