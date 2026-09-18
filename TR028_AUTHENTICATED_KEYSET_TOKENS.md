# TR-028 Authenticated SQL Keyset Tokens

Status: adopted as an opt-in SQL control.

## Problem

`ExecuteSQLQueryKeysetPage` already avoids deep `OFFSET` work, but its legacy
cursor is only JSON encoded and base64 wrapped. A caller can modify that value
before the query fingerprint check, and the legacy format has no expiry or
transport-size bound.

## API

Create a codec with a private secret and pass it on every page request:

```go
codec, err := hatSql.NewSQLKeysetTokenCodec(hatSql.SQLKeysetTokenCodecOptions{
    Secret: []byte("at-least-16-bytes-of-private-key"),
    MaxAge: 15 * time.Minute,
})
if err != nil {
    return err
}

result, err := hatSql.ExecuteSQLQueryKeysetPage(
    ctx,
    "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score",
    resolver,
    nil,
    hatSql.SQLQueryOptions{KeysetCursorTokenCodec: codec},
    100,
    cursor,
)
```

The codec signs the existing direct or partitioned cursor with HMAC-SHA256,
records its issue time, rejects tampering before query execution, and expires
tokens after `MaxAge`. Zero `MaxAge` uses the 15-minute default. Tokens do not
encrypt cursor contents and do not replace query authorization.

The option is deliberately opt-in. A nil `KeysetCursorTokenCodec` preserves
the existing legacy cursor format and behavior. When enabled, the same codec
must be supplied for the next page. The codec accepts cursors up to 96 KiB and
tokens up to 128 KiB; oversized values are rejected before allocation grows
without bound.

## Security and operations

- Keep the secret outside source control and configuration examples.
- Replay is possible until expiry; the token is a continuation credential, not
  a one-use nonce.
- The existing query and parameter fingerprint remains inside the signed
  cursor and is still checked after authentication.
- Partition layout validation remains active for partitioned sources.
- Secret rotation requires an application-level dual-reader policy if old
  cursors must remain valid during rollout.

## Measurement

Command: `make benchmark-tr028-keyset-token`.

Five samples on Linux amd64, AMD Ryzen 9 5950X, compare the current legacy
JSON/base64 cursor codec with the opt-in signed wrapper for the same cursor:

| Path | Median time | Heap | Allocations | Signed / legacy |
| --- | ---: | ---: | ---: | --- |
| Legacy encode | 708 ns/op | 497 B/op | 4 allocs/op | 1.00x baseline |
| Signed encode | 1,211 ns/op | 1,296 B/op | 9 allocs/op | 1.71x CPU, 2.61x heap, 2.25x allocs |
| Legacy decode | 2,123 ns/op | 504 B/op | 11 allocs/op | 1.00x baseline |
| Signed wrapper decode | 1,098 ns/op | 880 B/op | 8 allocs/op | 0.52x CPU, 1.75x heap, 0.73x allocs |

The representative cursor was 147 bytes in the legacy format and 262 bytes
when signed: 1.78x the wire size, or 115 additional bytes. Signed decoding
above measures authentication and envelope decoding only; SQL then performs
the existing legacy cursor JSON decode, so the enabled path adds that work to
the request. The default nil-codec path has no new CPU, heap, or bandwidth
cost. The tradeoff is therefore intentional: opt-in deployments exchange
bounded authenticated continuation and expiry for extra token bytes and
encode-time overhead.
