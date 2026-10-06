# M-U33 As-Of Compaction Admission

`hatPipeline.FrontierRetentionRegistry` already tracks bounded historical-read
leases and reports the safe compaction boundary. This feature adds an explicit
execution-time reservation so a storage callback can hold that boundary for
the complete delete/merge operation.

## Pipeline API

```go
permit, err := retention.BeginCompaction(ctx, "events", boundary)
if err != nil {
	return err
}
defer permit.Release()

return compactHistory(ctx, boundary)
```

`BeginCompaction` waits until the frontier lower bound reaches `boundary` and
all active leases are compatible with removing history strictly before it. A
successful permit makes `SafeCompactionBefore` return zero for that frontier
until `Release` is called. Release is idempotent. A zero boundary is an
explicit no-op permit.

The registry does not compact data, start a goroutine, or persist leases. A
caller that recreates a registry after restart must recreate its active
leases before allowing historical reads.

## Storage API

Use the opt-in controller helper when the storage callback performs the actual
history removal:

```go
job, accepted, err := controller.SubmitWithBoundaryAdmission(
	ctx,
	request,
	retention,
	"events",
	boundary,
)
```

The helper checks `ctx` before queueing. The context passed to
`controller.Run` controls a worker's admission wait. Immediately before the
callback starts, the controller obtains the permit; after the callback returns
it releases the permit even when the callback returns an error or panics. A
release error is returned when the callback itself succeeded.

The existing `CompactionController.Submit` path is unchanged. Applications
that do not opt into `SubmitWithBoundaryAdmission` pay no new work and no new
allocation.

## Operational Guidance

- Call the helper for every callback that can delete or rewrite historical
  versions.
- Keep the boundary used for admission equal to the boundary used by the
  storage engine.
- Use a cancellable `Run` context so a compaction waiting on frontier progress
  can be stopped.
- Treat `ErrFrontierRetentionClosed` and context cancellation as a failed
  maintenance attempt; retry only after the owning lifecycle is available.
- Do not expose the permit as a client-facing lease. It is an internal
  maintenance reservation and should be released in the same operation that
  acquired it.

## Tradeoff

This is a correctness feature, not a throughput optimization. On the measured
AMD Ryzen 9 5950X benchmark, the default controller path stayed at 1,065 ns/op,
184 B/op, and 4 allocs/op. The opt-in admission path measured 1,161 ns/op,
248 B/op, and 5 allocs/op: about 1.09x latency, +64 B, and +1 allocation per
scheduled compaction. The cost is confined to callers that explicitly choose
the admission helper; it was retained because it provides a concrete
execution-time boundary contract for storage engines.
