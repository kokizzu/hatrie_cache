# Bounded Pipeline Stages

`hat/hatPipeline` provides a small generic pipeline for workloads that benefit
from independent worker pools between processing stages.

## API

```go
pipeline, err := hatPipeline.NewPipeline(
    hatPipeline.Stage[Record]{
        Name: "normalize",
        Workers: 2,
        Queue: 64,
        Process: normalize,
    },
    hatPipeline.Stage[Record]{
        Name: "index",
        Workers: 4,
        Queue: 64,
        Process: index,
    },
)
if err != nil {
    return err
}

output, pipelineErrors := pipeline.Run(ctx, input)
for record := range output {
    handle(record)
}
for err := range pipelineErrors {
    return err
}
```

`Workers` controls the number of workers in a stage. `Queue` controls the
buffer between that stage and the next stage; zero means an unbuffered handoff.
Zero `Workers` uses one worker. A stage must provide `Process`, and negative
queue sizes are rejected with `ErrPipelineInvalid`.

The runner applies backpressure when a stage queue is full. The first processing
error is wrapped with the stage name, sent on the error channel, and cancels
the run. Context cancellation is also reported. Processing functions should
return promptly when their context is canceled. The output channel closes
before the error channel closes, so callers can drain output and then inspect
all reported errors without a send-after-close race.

## Measurement

The focused benchmark is available through `make benchmark-pipeline-stages`.
On the development host at implementation time, two stages with two workers,
queue size 32, and 1,000 integer values measured:

| Workload | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Two-stage pipeline, 1,000 values | 1,169,765 | 2,207 | 21 |

This measures pipeline coordination and channel handoff, not application work.
Tune worker and queue counts against the real workload because more workers can
increase scheduling overhead when processing is small.
