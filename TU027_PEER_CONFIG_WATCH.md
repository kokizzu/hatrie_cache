# T-U27 Peer Configuration Watch

`hatPeer.PeerConfigWatcher` is a bounded reconnecting cursor loop for remote
configuration prefixes. It is transport-neutral: an adapter can implement the
`PeerConfigWatchTransport` interface over `CompactPeerSession`, HTTP, or another
authenticated peer protocol.

```go
watcher, err := hatPeer.NewPeerConfigWatcher(transport, hatPeer.PeerConfigWatchOptions{
	Prefix: "config/db/",
	Handler: func(ctx context.Context, event hatPeer.PeerConfigWatchEvent) error {
		return applyConfig(event.Key, event.Value, event.Deleted)
	},
})
if err != nil {
	return err
}
return watcher.Run(ctx)
```

Each poll carries the prefix, the last successfully applied cursor, and a
bounded limit. Event values are copied before delivery. Events must match the
prefix, advance strictly beyond the current cursor, and not exceed the batch's
next cursor. A handler error leaves the rejected event at the current cursor so
the caller can retry it.

Transport errors reconnect with bounded exponential backoff. The cursor is
preserved across reconnects, and cancellation interrupts both polling and
backoff. `MaxReconnects` can cap attempts; zero means keep watching until the
context ends. Defaults are a 64-event batch, 10 ms initial backoff, and 1 s
maximum backoff.

Authentication, authorization, persistence, and remote log retention remain
transport/server responsibilities. The watcher does not log configuration
values and does not treat CRC or transport framing as authentication.

## Verification

```text
make test-tu27
make verify-tu27
make benchmark-tu27
```
