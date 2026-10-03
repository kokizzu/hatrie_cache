# Fiber-local storage

`hatFiber.Local[T]` provides typed state owned by one fiber slot. It is an
opt-in alternative to creating a new `context.WithValue` context for every
continuation step; it is not a replacement for cancellation, deadlines, or
request metadata carried by `context.Context`.

```go
var requestID hatFiber.Local[string]

step := func(ctx hatFiber.Context) (hatFiber.Step, error) {
	if err := requestID.Set(ctx, "req-42"); err != nil {
		return nil, err
	}
	return func(ctx hatFiber.Context) (hatFiber.Step, error) {
		id, ok := requestID.Get(ctx)
		if !ok {
			return nil, errors.New("request ID missing")
		}
		_ = id
		requestID.Delete(ctx)
		return nil, nil
	}, nil
}
```

The zero value is usable. `hatFiber.NewLocal[T]()` is provided when an
explicit pointer is more convenient. `Set`, `Get`, and `Delete` use the
`*Local[T]` identity as the slot key, so copied Local values are independent.

## Lifecycle contract

- A newly spawned fiber starts with no local values; values are never inherited
  from a prior fiber using the same scheduler slot.
- Values survive continuations of the same fiber.
- Completion, cancellation, and close clear every local entry before the slot
  can be reused. Oversized maps are discarded instead of retaining arbitrary
  key/value references; small maps retain capacity for reuse.
- A retained `Context` observes no value after its fiber is released. `Set`
  returns `ErrLocalInvalid` for a released or invalid context.
- Local operations are intended to run from the fiber's `Step`; a Context must
  not be shared concurrently for mutation by another goroutine.

The implementation uses a per-fiber lock and ownership generation, so local
operations on unrelated fibers do not serialize on the scheduler lock and a
stale context cannot address a reused slot.

## Measured tradeoff

Five samples with `GOMAXPROCS=1`, Linux/amd64, AMD Ryzen 9 5950X. The
pre-change baseline is the same T-U31 checkout using `context.WithValue` for a
per-step set plus lookup. Raw samples are preserved in
[`T032_BENCHMARK_RAW.txt`](T032_BENCHMARK_RAW.txt) and
[`T032_BENCHMARK_BASELINE_RAW.txt`](T032_BENCHMARK_BASELINE_RAW.txt).

| Workload | Pre-change context | Fiber local | Relative result |
| --- | ---: | ---: | --- |
| Set plus Get | 39.62 ns/op, 55 B/op, 1 alloc/op | 48.97 ns/op, 7 B/op, 0 alloc/op | 1.24x slower CPU, 7.86x lower bytes |
| Warm Get | 4.391 ns/op, 0 B/op, 0 alloc/op | 16.20 ns/op, 0 B/op, 0 alloc/op | 3.69x slower CPU, equal memory |

This is intentionally an allocation and retention optimization, not a claim
that local lookup beats the highly optimized read-only context lookup. The
feature remains opt-in and does not alter scheduler defaults or existing
`Context` value behavior.
