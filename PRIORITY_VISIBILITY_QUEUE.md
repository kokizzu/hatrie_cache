# Priority Visibility Queue

`hatDataStructure.PriorityVisibilityQueue[T]` is an importable, in-memory
priority work queue for consumers that need claim/acknowledge/retry semantics.
It combines lower-number-first priority ordering, delayed readiness, visibility
timeouts, retry attempt counts, and explicit durable checkpoints.

The queue is non-thread-safe, matching the other queues in
`hatDataStructure`. A caller that shares it between workers must provide its
own synchronization or partition work across queues.

## Semantics

- `Enqueue(priority, value)` makes a value ready immediately.
- `EnqueueAt` and `EnqueueAfter` delay visibility. Delayed work does not block
  already-ready work, even when its priority is higher.
- Among ready values, lower numeric priorities are leased first. Equal
  priorities are FIFO.
- `Lease` hides an item until the configured timeout. `LeaseWithToken` returns
  an epoch-fenced token for handoff across workers or processes.
- `Ack`/`AckToken` complete work. `Nack`/`NackToken` return it for a later
  retry while retaining its ID, priority, and attempt count.
- `RequeueExpired` makes timed-out leases ready again. `Lease` calls it before
  selecting work, so a polling worker does not need a timer goroutine.
- IDs are assigned at enqueue time and are not reused by `Clear`.
- A positive capacity counts pending and leased items. Zero is unbounded.
- The zero value is usable and uses the existing one-minute default timeout.

## Example

```go
now := time.Unix(100, 0).UTC()
queue := hatDataStructure.NewPriorityVisibilityQueue[string](1024, 30*time.Second)

queue.Enqueue(20, "normal")
queue.Enqueue(1, "urgent")
lease, ok := queue.LeaseWithToken(now)
if ok {
	if err := process(lease.Value); err == nil {
		queue.AckToken(lease.Token)
	} else {
		queue.NackToken(lease.Token, now.Add(2*time.Second))
	}
}
```

Use `Attempts` to apply a caller-owned retry policy and send exhausted work to
`DeadLetterQueue` or another durable workflow.

## Durable Checkpoints

The queue does not write on every mutation. This keeps the steady-state lease
path allocation-free and lets the caller choose its durability point. The
checkpoint includes pending values, active leases, IDs, priorities, retry
attempts, deadlines, capacity, timeout, and queue counters.

```go
codec := hatDataStructure.PriorityVisibilityQueueCodec[string]{
	Encode: func(value string) ([]byte, error) { return []byte(value), nil },
	Decode: func(payload []byte) (string, error) { return string(payload), nil },
}

if err := hatDataStructure.SavePriorityVisibilityQueue("jobs.hpq", queue, codec); err != nil {
	return err
}

restored, err := hatDataStructure.LoadPriorityVisibilityQueue("jobs.hpq", codec)
if err != nil {
	return err
}
// Load advances the epoch, so tokens issued before the restart are rejected.
```

`SavePriorityVisibilityQueue` writes a CRC-protected HPQ1 binary file through
a same-directory temporary file, `fsync`, and rename. The published file is
created with mode `0600`. `LoadPriorityVisibilityQueue` validates the checksum,
format, counts, counters, and value lengths before returning a queue. A
corrupt or truncated file never produces a partially restored queue.

`UnmarshalPriorityVisibilityQueue` preserves the serialized epoch for callers
that manage epoch ownership themselves. `RestorePriorityVisibilityQueue`
accepts an explicit epoch; pass a new value when restoring an in-memory
snapshot after a restart.

The default binary format is compact and codec-neutral. Use a fixed-width
binary or protobuf codec for typed values when wire/storage size matters. The
codec is responsible for validating its own payload and must not deserialize
untrusted data into executable objects.

The built-in bounds are one million entries, 64 MiB per encoded value, and
512 MiB per snapshot. Capacity and path policy remain caller-owned. For
per-mutation crash durability, checkpoint after each durable state transition
or pair the queue with the command journal; a checkpoint alone deliberately
does not provide a write-ahead log.

## Benchmark

Run:

```text
make benchmark-tt048-c296
```

The benchmark uses an AMD Ryzen 9 5950X host and five Go benchmark samples.
The lease comparison is a one-item enqueue/lease/ack loop. Snapshot cases
serialize 1,024 string values and report transient Go heap separately from the
actual snapshot bytes.

| Workload | Median ns/op | B/op | allocs/op | Snapshot bytes |
| --- | ---: | ---: | ---: | ---: |
| Existing `VisibilityQueue` lease + ack | 100.1 | 0 | 0 | n/a |
| `PriorityVisibilityQueue` lease + token ack | 128.2 | 0 | 0 | n/a |
| Binary snapshot with string codec | 195,142 | 237,904 | 1,040 | 53,304 |
| Standard JSON snapshot | 454,752 | 165,586 | 1,026 | 110,220 |

The priority queue is `1.28x` the CPU cost of the simpler existing queue
because it maintains priority and enqueue identity; neither path allocates in
steady state. The binary snapshot is `2.33x` faster and `2.07x` smaller than
the standard-library JSON snapshot, while using `1.44x` more transient heap
for deterministic snapshot assembly. That cost is paid at an explicit
checkpoint, not on normal lease/ack operations.

Raw samples:

```text
priority_lease_ack: 135.1, 128.2, 124.6, 133.1, 127.3 ns/op; 0 B/op; 0 allocs/op
visibility_lease_ack_baseline: 104.1, 106.3, 94.88, 100.1, 96.96 ns/op; 0 B/op; 0 allocs/op
priority_binary_string_marshal: 193190, 194024, 197409, 197952, 195142 ns/op; 53304 snapshot-bytes; 237904 B/op; 1040 allocs/op
priority_json_marshal: 439588, 463839, 452166, 454752, 455993 ns/op; 110220 snapshot-bytes; 165586 B/op; 1026 allocs/op
```
