# T-U27: Remote Prefix And Configuration Watches

Status: adopted as an opt-in compact-peer capability.

`hatPeer` now exposes a bounded watch protocol over an existing
`CompactPeerSession`. `hatCache.HatTrie` implements the provider interface by
adapting its local exact-key and prefix watchers. This is useful for remote
configuration invalidation and cache refresh consumers without creating a
second transport or bypassing the compact session's framing and authentication.

## Usage

The server is constructed with a provider. The connection must already have
completed the application's compact handshake and authorization policy before
the watch server is created.

```go
server, err := hatPeer.NewCompactPeerWatchServer(conn,
	hatPeer.CompactPeerWatchServerOptions{
		Session: hatPeer.CompactPeerSessionOptions{
			Context: ctx,
			Handler: ordinaryHandler,
		},
		Provider: trie, // *hatCache.HatTrie implements the provider.
	})
if err != nil {
	return err
}
defer server.Close()
```

The client registers an exact key or prefix. The default buffer is 64 events;
the request can set a smaller or larger bounded buffer. Coalescing is opt-in
and retains the latest event for each key in a delivery window.

```go
watch, err := client.Watch(ctx, hatPeer.CompactPeerWatchRequest{
	Prefix:   "cfg:",
	Buffer:   128,
	Coalesce: true,
})
if err != nil {
	return err
}
defer watch.Close()

for event := range watch.Events() {
	if event.Gap {
		refreshAllConfiguration()
		continue
	}
	refreshConfiguration(event.Key)
}
```

Create the client with `NewCompactPeerWatchClient`. When the transport is
replaced, the caller performs the normal handshake/authentication on the new
connection and passes it to `client.Reconnect(newConn)`. Reconnect re-registers
all active watches. It does not dial, authenticate, or hide connection policy.

```go
client, err := hatPeer.NewCompactPeerWatchClient(conn,
	hatPeer.CompactPeerWatchClientOptions{
		Session: hatPeer.CompactPeerSessionOptions{Context: ctx},
	})
if err != nil {
	return err
}
defer client.Close()

// After the caller has authenticated replacementConn:
if err := client.Reconnect(replacementConn); err != nil {
	return err
}
```

## Semantics

- Requests select exactly one non-empty `Key` or `Prefix`.
- `Set` and `Delete` events preserve the local watch order and carry a
  monotone mutation epoch.
- The `hatCache.HatTrie` adapter has no replay log. If a reconnect's
  `LastEpoch` differs from the current epoch, it emits one explicit event with
  `Gap=true` and `Operation="gap"`; consumers must refresh their state.
- A reconnect with the same epoch does not emit a gap event.
- Prefix watches inherit the local watcher's unpartitioned-trie restriction.
- The watch buffer, maximum watch count, key/prefix length, and event operation
  size are bounded before allocation. A full consumer path applies bounded
  backpressure instead of growing an unbounded queue.
- Late events from an old connection are fenced by a reconnect generation and
  cannot reach a reattached watcher.
- Ordinary compact requests remain available through the session handler.

The wire commands are versioned binary envelopes:
`_hat.peer.watch.register.v1`, `_hat.peer.watch.unregister.v1`, and
`_hat.peer.watch.event.v1`. Existing session framing, request correlation,
context cancellation, and authentication remain in force.

## Measurements

The raw normal-condition samples are in
`T027_BENCHMARK_BASELINE_RAW.txt` and `T027_BENCHMARK_RAW.txt`. They use five
samples on Linux/amd64 with the same host and `-benchmem`.

The controlled ten-sample single-CPU comparison is retained in
`T027_BENCHMARK_STABLE_RAW.txt`.

| Workload | Baseline median | Feature median | Ratio | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Ordinary compact call | 4,650 ns/op, 448 B/op, 8 allocs/op | 5,170 ns/op, 448 B/op, 8 allocs/op | 0.90x | No allocation regression; normal run was scheduler-noisy. |
| Remote watch event | Not available before | 6,705 ns/op, 560 B/op, 10 allocs/op | 1.30x vs feature compact call | New end-to-end mutation, delivery, and acknowledgement path. |

A separate ten-sample `GOMAXPROCS=1` control measured ordinary compact calls at
4,142 ns/op before and 3,895 ns/op after, or 1.06x before/after. The feature is
therefore accepted as an opt-in capability rather than a replacement for the
ordinary compact call path; its extra event-delivery cost is explicit and only
paid when a remote watch is used.

## Verification

```text
make round115-feature-test-t027
make round115-feature-race-t027
make round115-feature-vet-t027
make round115-feature-benchmark-t027
```

The focused tests cover initial prefix delivery, same-epoch reconnect, explicit
gap delivery after a mutation during disconnect, late-event generation fencing,
malformed envelopes, bounds, and ordinary handler preservation.

Remaining work includes a durable replay log/checkpoint contract, automatic
dialer and retry policy integration, and finer-grained authorization for
individual watch prefixes.
