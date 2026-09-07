# Work-Stealing Pool

`hat/hatPipeline` provides an opt-in `WorkStealingPool` for workloads made of
independent cooperative tasks. Each worker owns a deque. The owner takes its
newest task first; an idle worker takes the oldest task from another deque.

```go
pool, err := hatPipeline.NewWorkStealingPool(ctx, 4, 1024)
if err != nil {
	return err
}
defer pool.Close()

for _, task := range tasks {
	if err := pool.Submit(ctx, task); err != nil {
		return err
	}
}
return pool.Wait()
```

`queueCapacity` is the total number of waiting tasks across all deques. A
running task does not consume a queue slot. `Submit` selects workers in
round-robin order. `SubmitTo` can place work on a known deque when the caller
has a natural partition; idle workers may still steal it. `Close` rejects new
tasks and drains admitted work. `Cancel` stops workers, but task functions must
observe their context because tasks are not preempted.

`Stats` reports admitted tasks, completed task functions, and stolen tasks.
The existing `Scheduler` remains the lower-overhead shared-queue option and is
unchanged.

## Selection Guidance

Use this pool when queue locality matters or when a producer can temporarily
overload one worker while other workers are idle. Do not replace the existing
`Scheduler` for tiny, evenly distributed no-op tasks: deque locks, worker
coordination, and per-worker state cost more in that workload.

## Benchmark

The following comparison ran five samples per case with
`-benchtime=200ms`, four workers, and no-op tasks on an AMD Ryzen 9 5950X.
Each operation constructs a pool, submits the batch, closes it, and waits for
completion. Values are the observed minimum-to-maximum sample ranges.

| Batch | Work-stealing ns/op | Shared scheduler ns/op | Work-stealing bytes/op | Shared scheduler bytes/op | Work-stealing allocs/op | Shared scheduler allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 4,769-5,449 | 3,575-4,082 | 1,230-1,232 | 1,152-1,153 | 21 | 12 |
| 64 | 18,181-21,442 | 16,461-20,020 | 2,125-2,136 | 1,153-1,154 | 39-40 | 12 |
| 256 | 70,063-78,880 | 63,656-71,731 | 5,096-5,130 | 1,155-1,162 | 125-128 | 12 |

This benchmark is not a general throughput win. It validates the cost of the
new opt-in behavior and prevents presenting work stealing as a faster default.
The relevant correctness and race checks are:

```text
make test-work-stealing-full-clean
make test-work-stealing-package-race-clean
make benchmark-work-stealing-clean
```
