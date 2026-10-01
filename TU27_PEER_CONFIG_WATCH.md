# T-U27 Peer Configuration Watch

`hatPeer` now exposes the authenticated, bounded topology configuration watch
over the existing compact peer session. It is opt-in: constructing a handler
does not start a listener, goroutine, or background reconnect loop.

## Server

Create a `hatTopology.ConfigWatchLog` with an authorizer and attach the handler
to a `CompactPeerSession` or `CompactPeerListener`:

```go
log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
    HistoryLimit: 4096,
    Authorizer: func(ctx context.Context, auth hatTopology.ConfigWatchAuthorization) error {
        return authorizeConfigPrincipal(ctx, auth.Principal, auth.Action, auth.Key)
    },
})
if err != nil {
    return err
}

sessionOptions.Handler = hatPeer.NewCompactPeerConfigWatchHandler(log)
```

The source remains the authority for principal authorization, key/value size
limits, retention, and history-gap detection. Configure the existing peer
listener authorization and TLS options for network transport; the watch RPC
does not bypass those connection-level controls.

## Client

The client keeps a version cursor and sends an idempotent request on every
call. Calls are serialized per client so concurrent readers cannot duplicate
or reorder cursor advancement:

```go
watch, err := hatPeer.NewCompactPeerConfigWatchClient(
    hatPeer.CompactPeerConfigWatchClientOptions{
        Session:   session,
        Principal: "service-a",
        KeyPrefix: "region/ap/",
        Limit:     128,
        Reconnect: reconnectPeerSession,
    },
)
if err != nil {
    return err
}

events, err := watch.Read(ctx)
```

`Limit == 0` uses the topology package default. `Read` returns currently
retained events. `Wait` blocks until a matching event is available or the
context ends. A prefix cursor advances over unrelated retained keys, so a
client does not repeatedly scan keys outside its configured region.

Reconnect is caller-owned. When a session call fails and the context is still
active, the optional callback supplies one replacement session and the same
cursor request is retried once. The request is safe to resend because the
server only reads after a cursor; it does not acknowledge or mutate state.

## History Gaps

The log is bounded. If the cursor is older than retained history, `Read` or
`Wait` returns `*hatTopology.ConfigWatchGapError`, and the client exposes it
through `LastError()`:

```go
var gap *hatTopology.ConfigWatchGapError
if errors.As(err, &gap) {
    snapshot, snapshotErr := loadFreshConfigSnapshot(ctx)
    if snapshotErr != nil {
        return snapshotErr
    }
    applySnapshot(snapshot)
    if resetErr := watch.ResetCursor(gap.EarliestVersion - 1); resetErr != nil {
        return resetErr
    }
}
```

Install the fresh snapshot before resetting the cursor. The server does not
silently skip a gap or invent a full snapshot.

## Compact Wire Contract

The command is `_hat.peer.config.watch.v1`. The request contains:

| Field | Encoding |
| --- | --- |
| protocol version | one byte, currently `1` |
| wait flag | one byte, `0` or `1` |
| after version | unsigned varint |
| limit | unsigned varint, bounded by `MaxConfigWatchReadLimit` |
| principal | length-prefixed UTF-8 bytes, max 128 bytes |
| key prefix | length-prefixed UTF-8 bytes, bounded by the log key limit |

A successful response contains the protocol version, status byte, next cursor,
event count, and each event's version, delete flag, source, key, and value.
A gap response carries current, requested-after, and earliest-retained
versions. Unknown status values, truncated lengths, oversized fields,
non-monotone event versions, deleted events with values, and cursor regressions
are rejected.

## Measurements

Measured on the repository benchmark host with `make benchmark-round42-peer-watch`
and five benchmark samples:

| Path | Time | Allocated | Allocations |
| --- | ---: | ---: | ---: |
| direct in-process read | 103.2-105.6 ns/op | 112 B/op | 3/op |
| compact peer read over `net.Pipe` | 6.66-6.80 us/op | 936-937 B/op | 25/op |
| compact response encode | 110.2-111.4 ns/op | 88 B/op | 5/op |
| standard JSON response encode | 321.9-332.3 ns/op | 152 B/op | 3/op |

For the one-event fixture, the compact payload was 29 bytes and the JSON
payload was 90 bytes: 3.10x smaller, or 67.8% less bandwidth. Compact
encoding was about 2.96x faster and used 42.1% fewer allocated bytes, at the
cost of two additional allocations per encode. The full peer numbers include
session framing, goroutine scheduling, and `net.Pipe`; they are not a claim
about a production network or TLS latency.

## Verification

The focused tests cover prefix replay, cursor advancement, history gaps and
snapshot reset, reconnect replay, malformed event rejection, and compact wire
size. Focused race and vet targets pass. The broader package suite still has
unrelated pre-existing failures in compact frame-size assertion and the
mutual-TLS client-certificate test; those are recorded in the task report and
were not changed by this feature.
