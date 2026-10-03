# T-U27 Peer Configuration Watches

`hatPeer` provides an opt-in, bounded configuration watch over an existing
`CompactPeerSession`. The watch protocol is binary and revisioned. It does not
start a listener, enable a monitor, or change the existing peer transport by
itself.

## Contract

The server retains a bounded history of configuration events:

```go
server, err := hatPeer.NewCompactPeerConfigWatchServer(hatPeer.CompactPeerConfigWatchOptions{})
if err != nil {
	return err
}

// Install this handler in the server-side CompactPeerSession.
handler := server.Handler(nil)

revision, err := server.Publish("region/sg/cache/feature-flags", []byte("enabled=true"))
_ = revision
```

The client uses a prefix and a local revision cursor:

```go
watch, err := hatPeer.NewCompactPeerConfigWatchClient(
	hatPeer.CompactPeerConfigWatchClientOptions{Prefix: "region/sg/"},
)
if err != nil {
	return err
}

state, err := watch.Open(ctx, clientSession)
if err != nil {
	return err
}
_ = state // current revision and oldest retained revision

events, err := watch.Poll(ctx)
if err != nil {
	return err
}
for _, event := range events {
	// Apply event.Value for event.Path, then let Poll advance the cursor.
}
```

After a connection failure, call `Reconnect` with a new compact session. The
client reuses its last consumed revision, so retained matching events are
replayed in order. A cursor older than the retained history returns
`ErrCompactPeerConfigWatchHistoryGap`; the caller must refresh a full
configuration snapshot instead of silently continuing with incomplete state.

`Poll` is bounded and pull-based. There is no per-connection server queue and
no unbounded goroutine or callback backlog. Non-matching revisions advance the
cursor once the retained history has been fully scanned; matching events stop
at `MaxEvents` so later events remain replayable.

## Defaults

| Limit | Default | Purpose |
| --- | ---: | --- |
| `MaxHistory` | 256 | Retained events for reconnect replay |
| `MaxEvents` | 32 | Events returned by one poll |
| `MaxPrefixBytes` | 256 | Maximum watched prefix |
| `MaxPathBytes` | 1,024 | Maximum event path |
| `MaxValueBytes` | 65,536 | Maximum event value |

All limits are bounded. A client request larger than the server's
`MaxEvents` is clamped to the server limit. Oversized paths, values, malformed
payloads, future revisions, and replay gaps fail closed.

## Security And Operations

- Keep the existing listener authorization and TLS/mTLS configuration around
  the compact session. `Handler` performs protocol validation but does not
  authenticate callers.
- Treat event values as untrusted bytes. `Publish` copies caller buffers, and
  the decoder bounds every length before allocating.
- Persist or otherwise checkpoint the last applied revision with the consumer's
  configuration snapshot if recovery must survive process loss.
- Reconnect only after establishing a new authenticated session. If the server
  reports a history gap, perform a full snapshot/bootstrap before resuming.
- The watch is opt-in. Creating a normal `CompactPeerSession` without
  installing `server.Handler` has no watch overhead.

## Verification

The focused contract, malformed-payload, race, and wire-size tests are kept in
`hat/hatPeer/tu27_peer_watch_test.go`. The matched JSON and binary codec
benchmarks are in `hat/hatPeer/tu27_peer_watch_benchmark_test.go`.
