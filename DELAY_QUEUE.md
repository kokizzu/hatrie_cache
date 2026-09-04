# Delay Queue

`hatDataStructure.DelayQueue[T]` is a reusable in-memory delay queue for
values that become eligible at a deadline. It is inspired by queue primitives
used around Tarantool workers and is independent of the SQL and persistence
formats.

```go
queue := hatDataStructure.NewDelayQueue[string](128)
queue.Push(time.Now().Add(time.Minute), "send email")
queue.PushAfter(time.Now(), 5*time.Second, "refresh cache")

if value, ok := queue.PopReady(time.Now()); ok {
	_ = value
}
```

The zero value is usable. `Push`, `Peek`, `Pop`, `PopReady`,
`NextReadyAt`, `Len`, and `Clear` are provided. Equal deadlines preserve
insertion order. The queue is not internally synchronized; callers that share
one queue between goroutines must provide their own synchronization.

The implementation uses a contiguous 4-ary min-heap. `PopReady` returns no
item until the earliest deadline is reached, while `Pop` removes the earliest
item regardless of readiness. `Clear` drops references held by queued values.

## Measurement

Command: `make benchmark-delay-queue`.

The benchmark keeps 256 items resident and measures steady-state push/pop
operations against an equivalent `container/heap` implementation.

| Implementation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `DelayQueue` | 288.8 | 0 | 0 |
| `container/heap` reference | 293.7 | 96 | 2 |
| Improvement | **1.02x faster** | **96 fewer** | **2 fewer** |

Raw samples:

```text
delay_queue: 259.9, 277.7, 314.1, 298.4, 288.8 ns/op; 0 B/op; 0 allocs/op
container_heap_reference: 291.9, 292.8, 298.5, 309.7, 293.7 ns/op; 96 B/op; 2 allocs/op
```

| Implementation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `DelayQueue` | 286.9 | 0 | 0 |
| `container/heap` reference | 291.6 | 96 | 2 |
| Improvement | **1.02x faster** | **96 fewer** | **2 fewer** |

The CPU difference is intentionally modest; the primary benefit is keeping
steady-state scheduling out of the allocator while retaining predictable
`O(log n)` insertion and removal.
