# Journal Replay Progress

Recovery callers that need operator-visible progress can use the opt-in
`CommandJournal.ReplayWithProgress` method. It reports discovered work,
applied records, the current sequence, elapsed time, a best-effort remaining
time estimate, and a terminal error when replay fails.

```go
go func() {
	_, err := journal.ReplayWithProgress(restored, snapshotSequence, targetSequence)
	// handle err
}()

for journal.ReplayProgress().Active {
	progress := journal.ReplayProgress()
	// expose progress to the operator
}
```

`Total` counts non-checkpoint records after `snapshotSequence` and within the
requested target. `Applied` advances only after a command succeeds. `ETA` is
zero until at least one record has completed and is only an estimate; callers
must still wait for `ReplayWithProgress` to return.

The progress snapshot is safe to poll from another goroutine. It remains
available after completion, including partial counts and `Error` on failure.
The existing `Replay` and `ReplayThrough` methods do not enable tracking, so
their behavior and hot path are unchanged. The command journal format,
sequence validation, mutation ordering, and recovery semantics are unchanged.

This API is useful to embedding recovery tools and restore workflows. The
daemon's normal startup replay finishes before its monitoring listener starts,
so it is not presented as a misleading startup `/api/stats` gauge.
