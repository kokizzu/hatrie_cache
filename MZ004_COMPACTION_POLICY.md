# MZ-004 Per-Frontier Compaction Policy

`hatPipeline.FrontierCompactionScheduler` now supports an opt-in outstanding
task cap for each frontier/collection. The cap includes submissions waiting for
frontier safety and tasks already handed to the worker scheduler, so one hot
collection cannot fill the global queue while other collections make progress.

```go
if err := scheduler.SetPolicy("orders", hatPipeline.FrontierCompactionPolicy{
	MaxOutstanding: 2,
}); err != nil {
	return err
}
defer scheduler.ClearPolicy("orders")
```

Submissions blocked by the cap honor their context and `Cancel`. The default is
unchanged: no policy map is allocated until `SetPolicy` is called, and
unconfigured frontiers use the existing scheduler path.

## Measurement

Command: `make benchmark-mz004-compaction-policy` on Linux/amd64, AMD Ryzen 9
5950X, five samples, `-benchtime=10000x`.

| Path | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Default scheduler | 807 | 224 | 4 | 1.00x |
| `MaxOutstanding: 1` | 1,605 | 424 | 8 | 1.99x |

The policy is a concurrency and fairness control, not a single-task speedup.
It costs about 200 B and 4 allocations per configured submission in this
fixture. The default path has no measured change. Durable compaction jobs and
priority ordering remain future work.
