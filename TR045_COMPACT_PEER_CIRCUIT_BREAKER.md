# TR-045: Compact Peer Circuit Breaker

`hatPeer.NewCompactPeerCircuitBreaker` adds Tarantool-style failure admission
and health reporting around one `CompactPeerSession`. It is opt-in: existing
sessions and their successful-call path are unchanged unless the wrapper is
constructed.

```go
breaker, err := hatPeer.NewCompactPeerCircuitBreaker(session, hatPeer.CompactPeerCircuitBreakerOptions{
	Failures: 5,
	Cooldown: 30 * time.Second,
})
if err != nil {
	return err
}
response, err := breaker.Call(ctx, []byte("GET"), payload)
```

Zero `Failures` and `Cooldown` use five failures and a 30-second cooldown.
After the threshold, calls return `ErrCompactPeerCircuitOpen` without writing
to the socket. The cooldown permits one half-open probe. A successful probe
closes the circuit; a failed probe reopens it. Caller cancellation and deadline
errors are returned unchanged and do not count as peer failures.

`Status()` reports the circuit state, consecutive failures, a bounded local
health score, the open deadline, and the last failure reason. It does not
reconnect the session or replace application-level retry policy. The caller
still owns `CompactPeerSession.Close()`.

## Measurement

Linux amd64, AMD Ryzen 9 5950X, five samples per benchmark:

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Direct successful `Session.Call` | 6,774 | 448 | 7 |
| Opt-in breaker successful `Call` | 6,810 | 456 | 8 |
| Direct repeated remote error | 7,420 | 617 | 12 |
| Open breaker fail-fast call | 75.39 | 4 | 1 |

The wrapper is approximately 0.5% slower in this run and retains one additional
allocation and 8 bytes. Once a peer is known unhealthy, the open path is
approximately 98x lower latency, 154x lower bytes, and 12x fewer allocations
than issuing another remote call.

Measured with:

```text
make benchmark-tr045-peer
```
