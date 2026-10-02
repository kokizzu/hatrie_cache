# Peer Configuration Watch

`hatTopology` now exposes an opt-in prefix watch over an existing
`hatPeer.CompactPeerSession`.

## Server

Create a bounded, authorized log and bind the peer session to the identity
authenticated by the surrounding transport:

```go
log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
    Authorizer: authorizeConfig,
})
handler, err := hatTopology.NewConfigWatchPeerHandlerForPrincipal(log, "operator")
session, err := hatPeer.NewCompactPeerSession(conn, hatPeer.CompactPeerSessionOptions{
    Handler: handler,
})
```

`NewConfigWatchPeerHandlerForPrincipal` must be constructed only after the
connection has been authenticated. The client-supplied principal is ignored
for a bound session.

## Client

The client keeps a version cursor, retries transport failures with bounded
backoff, and applies prefix filtering on the server:

```go
watcher, err := hatTopology.NewConfigWatchPeerWatcher(ctx,
    hatTopology.ConfigWatchPeerOptions{
        Principal: "operator",
        Prefix:    "app:",
        Dial: func(ctx context.Context) (*hatPeer.CompactPeerSession, error) {
            return dialCompactPeer(ctx)
        },
    },
)
for event := range watcher.Events() {
    apply(event)
}
if err := watcher.Err(); err != nil {
    reconcile(err)
}
```

The default event buffer is 64 entries. A slow consumer applies bounded
backpressure; there is no unbounded client queue. `Close` cancels the current
wait and closes the event channel.

## Recovery

Every event has a monotonically increasing version. A reconnect resumes after
the last delivered version. If the server has already evicted that version
from its bounded history, the watcher terminates with `ConfigWatchGapError`.
It does not skip the gap. The operator must load a trusted snapshot, then
start a new watcher at the snapshot version.

Transport authentication remains the responsibility of the connection setup.
The config log authorizer handles principal and prefix authorization; the
compact protocol still enforces frame, payload, in-flight, and cancellation
bounds.

## Measurement

On the development machine, with 64 retained events and five benchmark runs:

| Operation | Median ns/op | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Local `ConfigWatchLog.Read` with prefix | 1,694 | 4,864 | 1 |
| Compact peer read round trip | 25,139 | 12,084 | 20 |

This is a control-plane feature, not a local read-path optimization. The
additional cost buys remote delivery, bounded reconnects, and explicit gap
handling. Run `make benchmark-tu27` for the raw five-run result.
