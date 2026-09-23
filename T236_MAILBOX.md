# T236 Mailbox Channels

`hat/hatFiber.NewMailbox[T]` is a bounded FIFO for independent worker
goroutines. It is separate from `hatFiber.Channel[T]`, which remains a
single-owner fiber primitive.

## Use

```go
mailbox, err := hatFiber.NewMailbox[Event](256)
if err != nil {
	return err
}
defer mailbox.Close()

if err := mailbox.Send(ctx, event); err != nil {
	return err
}

var batch [64]Event
count, err := mailbox.ReceiveBatch(batch[:])
if errors.Is(err, hatFiber.ErrMailboxEmpty) {
	// Wait for the first value, then drain any additional values.
	first, err := mailbox.Receive(ctx)
	if err != nil {
		return err
	}
	batch[0] = first
	count = 1
}
_ = count
```

`ReceiveBatch` is deliberately nonblocking so a consumer can integrate it
into an existing event loop. Use `Receive` when it must wait for the first
value. `TrySend` and `TryReceive` expose the nonblocking single-value paths.

## Properties

- The ring has a fixed capacity and does not allocate per element.
- `Send` and `Receive` are safe for concurrent producers and consumers.
- Blocking waits use context cancellation without starting a helper goroutine
  per waiter.
- `ReceiveBatch` drains several values while holding one lock, which is the
  main optimization for worker mailboxes.
- `Close` rejects new sends, wakes blocked operations, and allows buffered
  values to drain before returning `ErrMailboxClosed`.
- Mailbox capacity is explicit and bounded by `MaxMailboxCapacity` (`1<<20`),
  so configuration from an untrusted source cannot request unbounded storage.
- The mailbox uses one mutex and two one-token notification channels. This is
  intentionally simple and auditable; it is not a lock-free queue.

The zero value is invalid. Construct mailboxes with `NewMailbox` so the ring
and notification state are initialized together.

## Benchmark

Workload: four producer goroutines send 4,096 integers through a capacity-256
queue. The channel baseline receives one value at a time. The mailbox consumer
waits for the first value and then drains up to 64 values per batch. Results are
medians from five clean one-sample invocations on the same AMD Ryzen 9 5950X
host.

| Workload | Median ns/op | Median B/op | Allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Existing `chan` baseline | 228,010 | 2,573 | 7 | 1.00x |
| `Mailbox.ReceiveBatch` | 153,153 | 2,747 | 10 | 1.49x faster |

Raw samples:

```text
Channel ns/op:  228010 221741 228739 225040 237785
Channel B/op:   2601   2573   2570   2564   2596
Mailbox ns/op:  153478 150571 154307 144353 153153
Mailbox B/op:   2759   2747   2737   2740   2767
```

The mailbox uses about 6.8% more bytes/op and three additional allocations per
benchmark iteration in exchange for the 1.49x result on this batched workload.
The built-in channel remains appropriate for ordinary one-at-a-time messaging,
select-heavy code, or workloads that do not benefit from a batch drain.

Run the checks with:

```text
make test-t236
make race-t236
make vet-t236
make benchmark-t236
```
