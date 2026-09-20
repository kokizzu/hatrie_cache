# T-U32 Fiber-Local Storage

`hatFiber` provides opt-in typed storage scoped to the currently running
fiber:

```go
local, err := hatFiber.NewLocal[RequestState](scheduler)
```

`Local[T]` stores one typed value per scheduler slot. It is keyed by the full
generation-safe `FiberID`, so a slot reused after `Reap` cannot expose the
previous fiber's value.

## API

- `Get() (value T, ok bool, err error)` reads the current fiber's value.
- `Set(value T) error` stores a value for the current fiber.
- `Delete() error` clears the value and releases referenced data.

Calling these methods outside a running callback returns `ErrFiberNotRunning`.
An unset value returns `ok == false`; a stored zero value returns `ok == true`.
Values survive `StepYield` and `StepWait`, but are cleared automatically when
the fiber completes, fails, is canceled, or is reaped. The cleanup covers
reference-containing values and prevents state from being retained across
worker-slot reuse.

```go
type RequestState struct {
	Tenant string
	Trace  uint64
}

state, _ := hatFiber.NewLocal[RequestState](scheduler)

// Inside a StepFunc:
if value, ok, err := state.Get(); err != nil {
	return 0, err
} else if !ok {
	if err := state.Set(RequestState{Tenant: "eu", Trace: 42}); err != nil {
		return 0, err
	}
} else {
	value.Trace++
	if err := state.Set(value); err != nil {
		return 0, err
	}
}
```

Use one `Local[State]` for related values when practical. Each `Local` has a
fixed cell array sized to `scheduler.Capacity()` so `Get` and `Set` avoid map
lookups and steady-state allocations. The cost is proportional to the
configured fiber capacity and is paid when the local is created. The benchmark
in [BENCHMARK.md](BENCHMARK.md#t-u32-fiber-local-storage) measures this setup
cost alongside hot access.

`Local` is single-owner like the scheduler and is not safe for concurrent
access from another goroutine. Existing request contexts, goroutine locals,
and scheduler defaults are unchanged; this is an explicit fiber-only storage
contract.
