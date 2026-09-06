# Pipeline Scopes

`hat/hatPipeline` exposes `Scope` for operators that start their own worker
groups. A scope owns a cancellable context, accepts workers until `Wait` or
`Close`, and returns the first worker error. `GoChild` runs a nested scope as a
single parent worker, so the parent cannot finish before the child's workers
have stopped.

```go
root := hatPipeline.NewScope(ctx)
defer root.Close()

if err := root.GoChild(func(child *hatPipeline.Scope) error {
	return child.Go(func(ctx context.Context) error {
		return runOperator(ctx)
	})
}); err != nil {
	return err
}
return root.Wait()
```

`Cancel` propagates through every nested scope. Worker callbacks should observe
their context and return promptly after cancellation. `Wait` is idempotent but
must not be called from a worker belonging to the same scope. A scope uses one
context, wait group, and cancellation path per nesting level; it does not add
coordination to existing `Pipeline.Run` calls.

The feature is library-level and does not change the default pipeline runtime.
Existing callers can adopt it operator by operator.

## Verification

```text
make test-m003-clean
```
