# MZ-038 Dynamic Dataflow Worker Scaling

`hatPipeline.NewResizablePipeline` adds an opt-in dataflow pipeline whose
stage worker counts can change while a run is active. The existing immutable
`hatPipeline.Pipeline` remains unchanged and is still the lower-overhead
default when stage capacity is known in advance.

```go
pipeline, err := hatPipeline.NewResizablePipeline(
    hatPipeline.ResizablePipelineStage[int]{
        Name: "transform",
        Workers: 1,
        Queue: 64,
        Process: func(ctx context.Context, value int) (int, error) {
            return value * 2, nil
        },
    },
)
output, run, err := pipeline.Run(ctx, input)
if err != nil {
    return err
}
if err := run.ResizeStage(0, 4); err != nil {
    return err
}
for value := range output {
    consume(value)
}
return run.Wait()
```

`Workers: 0` selects one worker. `Queue: 0` selects an unbuffered stage
handoff. Stage queues are bounded by `DefaultResizablePipelineMaxQueue`; the
default maximum is 1 MiB entries. A resize never preempts an active callback,
and queued values are retained during a downscale. Processing errors are
reported once through `Errors()` and cancel the complete run. Callbacks should
observe their context when they can block.

This is worker scaling, not automatic sharding, data movement, or state
rebalancing. The caller chooses when to resize and owns any state migration.
The normal `Pipeline` path is unaffected.

## Measured Cost

The benchmark processes 256 values through two stages. The fixed baseline uses
the existing `Pipeline` with four workers per stage. The resizable fixed-worker
case uses the new API with four workers from the start. The scale-up case starts
with one worker per stage and resizes both stages to four before draining the
input. Results are five 500 ms runs on Linux amd64, AMD Ryzen 9 5950X:

| Operation | Median ns/op | B/op | allocs/op | CPU vs fixed | Bytes vs fixed |
| --- | ---: | ---: | ---: | ---: | ---: |
| existing fixed `Pipeline` | 182,655 | 4,647 | 24 | 1.00x | 1.00x |
| resizable, fixed workers | 235,839 | 4,472 | 29 | 1.29x | 0.96x |
| resizable, scale 1 -> 4 | 231,842 | 4,708 | 33 | 1.27x | 1.01x |

The resizable path has a bounded control and worker-management cost, so it is
appropriate when load changes justify resizing. Use the immutable pipeline for
steady-state workloads where that capability is unnecessary. Raw output is
recorded in [BENCHMARK.md](BENCHMARK.md#mz-038-dynamic-dataflow-worker-scaling).
