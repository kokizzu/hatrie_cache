# Replica Read Hedging

`hatTopology` provides an opt-in request hedger for ordered, read-only replica
candidates. It starts the preferred candidate immediately, starts a bounded
fallback after a delay when the first read is still pending, and cancels the
shared child context after the first successful result.

The topology router does not enable this automatically. The caller chooses
when duplicate read load is acceptable and supplies candidates returned by its
trusted routing policy. Candidate values can be node IDs, topology records,
or any other caller-owned type.

## Example

```go
candidates := []string{"node-a", "node-b"} // already ordered by the caller's routing policy

policy, err := hatTopology.NewReplicaHedgePolicy(hatTopology.ReplicaHedgePolicyOptions{
	MaxAttempts: 2,
	HedgeDelay:  5 * time.Millisecond,
})
if err != nil {
	return err
}

value, selectedIndex, err := hatTopology.ExecuteReplicaHedged(ctx, policy, candidates,
	func(ctx context.Context, candidate string) (string, error) {
		return readFromPeer(ctx, candidate, key)
	})
if err != nil {
return err
}
_ = candidates[selectedIndex]
return value
```

The callback must honor its context. A callback that ignores cancellation can
continue consuming resources after another replica has won. Candidates are
already ordered by the caller; the hedger does not probe nodes, change
topology, perform failover, or repair stale data.

## Semantics

- `MaxAttempts` defaults to 2 and is capped at 4. The first candidate counts
  as an attempt.
- `HedgeDelay` defaults to 5 ms and is capped at one hour.
- A later candidate starts after the delay only while an earlier attempt is
  pending. If every started attempt fails, the next candidate starts
  immediately.
- The first successful callback result wins. All other callbacks receive the
  canceled shared context.
- Only read operations are appropriate. Do not use this primitive for writes,
  non-idempotent commands, or reads with externally visible side effects.
- A single candidate or `MaxAttempts: 1` uses an inline fast path with no
  coordinator allocation on success.
- When all started attempts fail, the returned `*ReplicaHedgeError` retains
  candidate indexes and failures in candidate order. `errors.Is` matches
  `ErrReplicaHedgeAllFailed` and each underlying error.
- Cancellation before the first attempt returns the caller's context error
  without invoking the callback.

An observer can record fallback starts for metrics. Keep it lightweight; it is
called synchronously by the coordinator:

```go
policy, err := hatTopology.NewReplicaHedgePolicy(hatTopology.ReplicaHedgePolicyOptions{
	Observer: func(event hatTopology.ReplicaHedgeEvent) {
		hedgesTotal.Add(1)
		hedgeDelay.Observe(event.Delay.Seconds())
	},
})
```

The remote read handler remains responsible for authentication, authorization,
tenant isolation, and any consistency token or minimum applied sequence. The
hedger accepts only the candidate list supplied by the caller and does not
accept network addresses directly.

## Benchmark

Measured locally on Linux/amd64 with an AMD Ryzen 9 5950X. Each row is the
median of five `go test -benchmem` runs. The slow-first workload uses a
5 ms context-aware delay on the preferred candidate and an immediate second
candidate; the sequential and hedged rows use the same callback.

| Workload | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---|
| Sequential slow-first fallback | 5,121,417 | 248 | 3 | Baseline |
| Hedged slow-first fallback | 1,064,228 | 1,070 | 13 | 4.81x lower latency; 4.31x more bytes; 4.33x more allocations |
| Hedged healthy preferred replica | 1,167 | 616 | 8 | Coordination cost when no fallback is needed |
| Hedged single candidate, final fast path | 12.42 | 0 | 0 | No coordinator allocation |

The latency result is useful for a long-tail replica, but it is not a free
optimization: a fallback may execute duplicate work before cancellation
arrives. Choose the hedge delay from observed replica latency, keep the
attempt cap small, and monitor fallback rate and downstream read capacity.

Raw benchmark commands:

```text
make benchmark-t-u49-baseline
make benchmark-t-u49
```

Focused verification:

```text
make test-t-u49
make verify-t-u49
```
