# Late-Data Policy

`hat/hatSql` exposes `ClassifyLateData` for event-time workflows that need an
explicit bounded-lateness decision.

```go
decision, err := hatSql.ClassifyLateData(eventTime, watermark, hatSql.LateDataPolicy{
    AllowedLateness: 5 * time.Minute,
    DropTooLate:     true,
})
if err != nil {
    return err
}
if !decision.Accepted {
    return nil // route to a late-data metric or dead-letter path
}
applyEvent(event)
```

Events at or after the watermark are on time. Events before it are marked
`Late`, and `Lateness` reports how far behind the watermark they are. An event
whose lateness is greater than `AllowedLateness` is marked `TooLate`. The
boundary itself is accepted. `DropTooLate` controls only admission: when it is
false, too-late events remain accepted for correction, audit, or replay
workflows.

The helper is pure, allocation-free on valid inputs, and does not advance a
watermark or mutate a query. A negative allowed-lateness value is rejected
with `ErrLateDataPolicyInvalid`.
