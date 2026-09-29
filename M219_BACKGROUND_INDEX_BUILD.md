# M219 Background Index Creation

M219 adds the importable `hatSql.SQLBackgroundIndexBuilder`. It applies
caller-supplied differential batches on one background goroutine and exposes a
race-free status snapshot with processed rows, applied batches, and a monotone
logical build frontier.

The builder is arrangement-agnostic. A caller can apply batches to a private
`IncrementalPointLookup`, ordered index, or another SQL data structure, then
atomically publish that private state from the `Publish` callback:

```go
staging, _ := hatSql.NewIncrementalPointLookup(hatSql.IncrementalPointLookupDefinition{
	IndexKey: func(row hatSql.Row) (string, error) {
		return row["region"].(string), nil
	},
})
	builder, err := hatSql.NewSQLBackgroundIndexBuilder(
	hatSql.SQLBackgroundIndexBuildDefinition{
		Name: "accounts-by-region",
		Batches: []hatSql.SQLBackgroundIndexBuildBatch{
			{Frontier: 100, Updates: snapshotBatch1},
			{Frontier: 200, Updates: snapshotBatch2},
		},
		Apply: staging.Apply,
		Publish: func() error {
			// Swap staging into the caller-owned registry here.
			return nil
		},
	}, hatSql.SQLBackgroundIndexBuildOptions{})
if err != nil {
		return err
	}
if err := builder.Start(ctx); err != nil {
		return err
	}
status := builder.Status()
_ = status.BuildFrontier
return builder.Wait(ctx)
```

## Lifecycle

The states are `queued`, `running`, `ready`, `failed`, and `canceled`.
`BuildFrontier` advances only after the corresponding `Apply` callback returns.
`TargetFrontier` is derived from the final batch unless explicitly supplied;
non-empty builds reject a target that is not the final batch frontier. Empty
builds can advance directly from `InitialFrontier` to `TargetFrontier`.

`Publish` runs only after every batch succeeds and the build context is still
active. Apply or publish errors produce `failed`; context cancellation produces
`canceled`. A failed or canceled build is never reported as `ready`, so callers
can safely keep the staging arrangement unpublished.

The builder copies batch slices but does not clone row maps by default. The
default is intended for immutable source snapshots and avoids doubling row-map
memory. Set `SQLBackgroundIndexBuildOptions.CloneInputs` when the caller may
mutate input rows before completion; that safe mode clones row maps at build
creation and has a measurable cost.

The feature is opt-in. It does not start workers automatically, modify the
default SQL executor, or wire planner selection to index lifecycle. Callers
own source snapshot consistency, staging arrangement ownership, publication,
and any durable checkpoint.

## Measurement

Linux `amd64`, AMD Ryzen 9 5950X, five samples per benchmark,
`-benchtime=200ms`, 4,096 differential rows in 32 batches. The synchronous
control applies the same batches directly to a fresh point lookup. The
background path includes builder construction, one worker, status updates, and
`Wait`.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative to synchronous |
| --- | ---: | ---: | ---: | --- |
| Synchronous apply | 5,426,655 | 3,208,669 | 12,859 | 1.00x |
| Background builder, default immutable inputs | 5,341,309 | 3,382,212 | 12,896 | 0.98x CPU; 1.05x bytes; +37 allocs |
| Background builder, `CloneInputs: true` | 7,990,831 | 4,930,557 | 21,121 | 1.47x CPU; 1.54x bytes; +8,262 allocs |
| Status snapshot | 13.55 | 0 | 0 | allocation-free observation |

The default path keeps build throughput close to synchronous construction while
letting the foreground caller return from `Start` immediately and observe
progress. Full raw samples are retained in the benchmark output from
`make benchmark-m219`.

Raw samples from the final run:

```text
BenchmarkM219BackgroundIndexBuild/synchronous_apply-32         44 5376835 ns/op 3208769 B/op 12859 allocs/op
BenchmarkM219BackgroundIndexBuild/synchronous_apply-32         40 5316940 ns/op 3208771 B/op 12859 allocs/op
BenchmarkM219BackgroundIndexBuild/synchronous_apply-32         49 6026054 ns/op 3208669 B/op 12859 allocs/op
BenchmarkM219BackgroundIndexBuild/synchronous_apply-32         46 5736781 ns/op 3208633 B/op 12859 allocs/op
BenchmarkM219BackgroundIndexBuild/synchronous_apply-32         45 5426655 ns/op 3208641 B/op 12859 allocs/op
BenchmarkM219BackgroundIndexBuild/background_builder-32        39 5319042 ns/op 3382224 B/op 12896 allocs/op
BenchmarkM219BackgroundIndexBuild/background_builder-32        50 5103213 ns/op 3382312 B/op 12896 allocs/op
BenchmarkM219BackgroundIndexBuild/background_builder-32        34 6073390 ns/op 3382183 B/op 12896 allocs/op
BenchmarkM219BackgroundIndexBuild/background_builder-32        40 6048622 ns/op 3382187 B/op 12896 allocs/op
BenchmarkM219BackgroundIndexBuild/background_builder-32        44 5341309 ns/op 3382212 B/op 12896 allocs/op
BenchmarkM219BackgroundIndexBuild/background_builder_clone_inputs-32 49 8074986 ns/op 4930637 B/op 21121 allocs/op
BenchmarkM219BackgroundIndexBuild/background_builder_clone_inputs-32 26 7990831 ns/op 4930527 B/op 21121 allocs/op
BenchmarkM219BackgroundIndexBuild/background_builder_clone_inputs-32 36 7267762 ns/op 4930584 B/op 21120 allocs/op
BenchmarkM219BackgroundIndexBuild/background_builder_clone_inputs-32 28 8140492 ns/op 4930557 B/op 21121 allocs/op
BenchmarkM219BackgroundIndexBuild/background_builder_clone_inputs-32 31 7757726 ns/op 4930542 B/op 21121 allocs/op
BenchmarkM219BackgroundIndexStatusSnapshot-32 15764346 13.53 ns/op 0 B/op 0 allocs/op
BenchmarkM219BackgroundIndexStatusSnapshot-32 17991780 13.68 ns/op 0 B/op 0 allocs/op
BenchmarkM219BackgroundIndexStatusSnapshot-32 18210788 13.41 ns/op 0 B/op 0 allocs/op
BenchmarkM219BackgroundIndexStatusSnapshot-32 17042638 13.72 ns/op 0 B/op 0 allocs/op
BenchmarkM219BackgroundIndexStatusSnapshot-32 16055433 13.55 ns/op 0 B/op 0 allocs/op
```

## Verification

Focused correctness tests cover monotone frontier publication, cancellation,
failed validation, empty target advancement, and input cloning. The race test
also exercises concurrent status observation:

```text
make test-m219-red
make test-m219-green
make gofmt-m219
make race-m219
make benchmark-m219
```
