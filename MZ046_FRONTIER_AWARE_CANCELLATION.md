# MZ-046 Frontier-Aware Cancellation

`hatPipeline.FrontierCancellation` derives a query context that is canceled
when a registered frontier's lower bound reaches a requested target. It lets a
caller stop work once a freshness/result frontier is available without adding
frontier polling to the query loop.

## Usage

```go
watcher, err := hatPipeline.NewFrontierCancellation(
	parent,
	frontiers,
	"orders",
	targetSequence,
)
if err != nil {
	return err
}

queryErr := runQuery(watcher.Context)
watcher.Cancel()
watchErr := watcher.Wait()
if queryErr != nil && !errors.Is(queryErr, context.Canceled) {
	return queryErr
}
if watchErr != nil && !errors.Is(watchErr, context.Canceled) {
	return watchErr
}
return nil
```

When the lower frontier reaches `targetSequence`, `Wait` returns `nil` and the
derived context is canceled. If the query finishes first, call `Cancel` before
`Wait`; that returns `context.Canceled`. Registry closure and frontier removal
are reported as `ErrFrontierClosed` and `ErrFrontierNotFound` respectively.

The constructor rejects nil registries, empty IDs, and unregistered IDs. A
target already covered at construction uses a no-goroutine fast path. An
unresolved target owns one watcher goroutine until the target, parent context,
manual cancellation, or registry termination completes it.

This is an importable pipeline primitive. Existing SQL execution and frontier
behavior remain unchanged; callers decide which query or connector context to
derive.

## Benchmark

Environment: Linux amd64, AMD Ryzen 9 5950X 16-Core Processor. Command:
`make round69-mz046-bench`. Five samples were collected for each subcase.

| Subcase | Samples (ns/op) | Median | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Existing `WaitUntil`, already ready | 11.39, 11.63, 11.85, 11.79, 11.19 | 11.63 | 0 | 0 |
| `FrontierCancellation`, already ready | 224.8, 222.2, 226.0, 226.2, 225.7 | 225.7 | 272 | 4 |
| `FrontierCancellation`, pending then canceled | 714.2, 698.0, 698.2, 694.7, 700.3 | 698.2 | 320 | 5 |

The ready fast path is about `19.4x` slower than a direct ready check because
it preserves a derived context and watcher handle. The pending path pays for
the goroutine and cancellation state needed to interrupt in-flight work. This
cost is opt-in; normal frontier reads and existing query paths are unchanged.
