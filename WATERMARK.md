# Watermark Propagation

`hat/hatSql` provides small pure helpers for stream-style event-time
frontiers.

```go
safe, err := hatSql.MergeWatermarks(sourceWatermarks)
if err != nil {
    return err
}
published = hatSql.AdvanceWatermark(published, safe)
```

`MergeWatermarks` returns the minimum source watermark. This is the safe
downstream frontier because an operator must not consider a time complete
while any input source is still behind it. Empty input is rejected with
`ErrWatermarkInvalid`.

`AdvanceWatermark` is monotonic: a restarted or delayed source cannot move a
published frontier backward. Both helpers are pure, allocation-free on valid
inputs, and do not retain or mutate the caller's slice or timestamps.
