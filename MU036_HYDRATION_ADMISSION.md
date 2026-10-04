# M-U36 Hydration Progress And Admission

Typed-table aggregate and join arrangements now expose a first-class
hydration status and an admission barrier for readers that require complete
arrangement state.

```go
status, err := arrangement.HydrationStatus()
if err != nil {
    return err
}
if !status.Ready {
    if err := arrangement.WaitHydrated(ctx); err != nil {
        return err
    }
}
rows := arrangement.Rows()
```

Aggregate status reports its checkpoint, source sequence, remaining changes,
readiness, and the last hydration error. Join status reports the same fields
for both inputs. `WaitHydrated` returns a hydration error instead of admitting
a read from an arrangement that cannot catch up, and returns the caller's
context error when cancellation or a deadline wins.

The notification channel is created only while at least one caller is waiting.
Ordinary `Apply` and `Hydrate` calls keep the existing no-waiter allocation
profile. Successful application clears the last error and wakes waiters;
compaction gaps and other hydration errors are retained in status and wake
waiters so they fail deterministically.

`Freshness` remains the lighter boolean/checkpoint probe for hot paths. The
richer status snapshot is intentionally an opt-in admission/diagnostic call.
