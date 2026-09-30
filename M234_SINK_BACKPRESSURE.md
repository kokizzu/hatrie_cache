# M234 Bounded Sink Pending Output

M234 adds an opt-in FIFO for sink outputs that must remain available until an
external delivery is confirmed. It bounds both the number of pending outputs
and their retained payload plus idempotency-key bytes.

## Why

An unbounded pending slice can grow until a slow or disconnected sink consumes
the process memory. `SinkPendingOutputQueue` makes admission explicit:

- `Enqueue` waits for capacity and honors context cancellation.
- `TryEnqueue` returns immediately with `ErrSinkPendingOutputFull`.
- `Peek` returns the oldest output without removing it.
- `Acknowledge` removes only the current head after successful delivery.
- `Close` rejects new output but allows already admitted output to drain.

The queue is independent from `SinkBackpressureRegistry`. Use the registry for
frontier-lag admission and this queue for the actual pending-output memory
limit when both forms of protection are needed.

## Example

```go
queue, err := hatPipeline.NewSinkPendingOutputQueue(
    hatPipeline.SinkPendingOutputQueueOptions{
        MaxItems: 1024,
        MaxBytes: 64 << 20,
    },
)
if err != nil {
    return err
}
defer queue.Close()

sequence, err := queue.Enqueue(ctx, hatPipeline.SinkPendingOutput{
    Frontier:       frontier,
    IdempotencyKey: idempotencyKey,
    Payload:        payload,
})
if err != nil {
    return err
}

pending, ok, err := queue.Peek(ctx)
if err != nil || !ok {
    return err
}
if err := deliver(pending); err != nil {
    return err
}
return queue.Acknowledge(sequence)
```

`Enqueue` copies the payload before returning, so the caller can reuse its
buffer. `Peek` also returns a payload copy. Strings are immutable in Go, so
the idempotency key does not need a second allocation. The byte statistic is
the sum of key and payload lengths; the item bound also limits queue metadata.

## Defaults and limits

Zero options select `1024` items and `64 MiB`. Configured limits must be
positive and are capped at `65536` items and `1 GiB`. One idempotency key is
limited to `1024` bytes. A single output that cannot fit the configured byte
limit returns `ErrSinkPendingOutputTooLarge` before it waits.

The queue starts with a 256-entry metadata ring and grows only as needed up to
`MaxItems`. Acknowledge clears released slots, so payload and key backing
storage are eligible for collection as soon as an output is removed.

## Correctness rules

- The caller must enqueue with `Sequence == 0`; the queue assigns a monotonic
  sequence.
- Acknowledging a non-head sequence returns
  `ErrSinkPendingOutputNotHead`; this prevents an out-of-order delivery from
  removing an earlier output.
- Closing is idempotent. Producers are rejected after close, while `Peek`
  continues returning already queued outputs until they are acknowledged.
- Waiting producers and consumers are woken by capacity changes, close, or
  their context cancellation.

## Measurements

Five 200 ms samples were collected on an AMD Ryzen 9 5950X,
`linux/amd64`, with 256 outputs per iteration, an 18-byte payload, and a
9-byte key. The baseline is an unbounded `[]SinkPendingOutput` that performs
the same payload copies. Lower is better for every column.

| Workload | Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU | Relative cumulative allocation |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| enqueue and acknowledge 256 outputs | Existing unbounded slice | 14,583 | 14,336 | 768 | 1.00x | 1.00x |
| enqueue and acknowledge 256 outputs | M234 bounded queue | 16,083 | 6,144 | 256 | 1.10x | 0.43x |
| create and retain 256 outputs | Existing unbounded slice | 16,286 | 14,336 | 768 | 1.00x | 1.00x |
| create and retain 256 outputs | M234 bounded queue | 15,894 | 22,752 | 259 | 0.98x | 1.59x |

The queue removes key-copy allocations and avoids notification-channel
allocation when no goroutine is waiting. The retained-workload `B/op` column
is cumulative allocation reported by Go, not a direct live-heap measurement;
the bounded queue's operational guarantee is the configured item and byte cap,
not lower raw-slice overhead in every workload.

Raw samples are in [`M234_BENCHMARK_RAW.txt`](M234_BENCHMARK_RAW.txt). Repeat
the focused checks with:

```text
make test-m234
make race-m234
make benchmark-m234
```
