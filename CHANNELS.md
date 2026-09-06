# Typed Pipeline Channels

`hat/hatPipeline` provides `Channel[T]`, a small typed producer-consumer
wrapper around a Go channel. It is useful when a pipeline needs explicit
buffering, context-aware backpressure, and a safe close contract without
introducing another queue implementation.

```go
events, err := hatPipeline.NewChannel[Event](256)
if err != nil {
	return err
}
defer events.Close()

if err := events.Send(ctx, event); err != nil {
	return err
}

event, ok, err := events.Receive(ctx)
if err != nil {
	return err
}
if !ok {
	return nil // the channel was closed and fully drained
}
_ = event
```

`Send` waits until the buffer has room or the context is canceled. `Receive`
waits for a value or cancellation. Closing is idempotent: buffered values are
returned first, then `Receive` returns `ok == false`, while sends return
`ErrChannelClosed`. A nil context means `context.Background()`.

`In()` exposes the read-only channel for consumers that need a `select`, and
`Len()`/`Capacity()` provide low-cost buffer observations. A negative capacity
returns `ErrChannelInvalid`.
