# Deterministic Parallel Map

`hat/hatPipeline.OrderedMap` runs independent work concurrently while merging
results in input order. Workers claim indexes from one atomic cursor and write
to the corresponding output slot, so no completion-order sort or result map is
needed.

```go
results, err := hatPipeline.OrderedMap(ctx, rows, 4,
	func(ctx context.Context, row Row) (Summary, error) {
		return summarize(ctx, row)
	})
```

The function validates a positive worker count and callback, caps workers at
the input length, cancels remaining cooperative work after the first callback
error, and returns results in exactly the same order as `rows`. An empty input
returns a nil result and nil error. The callback must observe its context for
prompt cancellation.

This is an opt-in deterministic merge primitive for independent dataflow
operators. It is not a replacement for sequential loops on tiny or cheap
items: the worker goroutines, atomic cursor, cancellation context, and output
slice cost more than a direct loop in that case. The existing `Pipeline`,
`Scheduler`, and `WorkStealingPool` APIs are unchanged.

## Verification And Cost

The local benchmark maps 1,024 integers with four workers and compares the
same operation in a sequential loop. Five samples were run on an AMD Ryzen 9
5950X.

| Path | Time | Heap | Allocs |
| --- | ---: | ---: | ---: |
| Ordered parallel map | 14,926-16,352 ns/op | 8,857-8,858 B/op | 11/op |
| Sequential loop | 2,019-6,775 ns/op | 8,192 B/op | 1/op |

The benchmark is intentionally a cheap-work warning, not a speed claim. Use
the primitive when parallel callback work dominates coordination cost and
deterministic output order matters.

```text
make test-ordered-map-full-clean
make test-ordered-map-race-clean
make benchmark-ordered-map-clean
```
