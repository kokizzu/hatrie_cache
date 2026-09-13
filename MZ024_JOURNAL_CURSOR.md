# MZ-024 Journal Tail Cursors

`GET /api/journal` can return a signed continuation cursor for clients that
need to reconnect or page through a changing command journal without storing a
plain sequence in application state.

## Enable

The feature is disabled by default. Embedded users configure the monitoring
handler with a secret of at least 16 bytes:

```go
handler := hatriecache.NewMonitoringHandler(trie, hatriecache.MonitoringOptions{
	Journal:             journal,
	JournalCursorSecret: "replace-with-a-random-secret",
})
```

The daemon exposes the same setting as:

```text
-monitoring-journal-cursor-secret <secret>
```

The value is redacted by `-print-config`. Use a secret manager or an external
configuration mechanism rather than putting it in shell history or source
control.

## HTTP Contract

Request the first page normally:

```text
GET /api/journal?limit=100
```

When more records are available and the feature is enabled, the JSON response
contains `next_cursor`:

```json
{
  "last_sequence": 250,
  "limit": 100,
  "has_more": true,
  "next_cursor": "<signed-token>",
  "entries": []
}
```

Use that value for the next page:

```text
GET /api/journal?cursor=<signed-token>&limit=100
```

`cursor` and `after_sequence` are mutually exclusive. The binary journal-tail
response keeps its existing payload format and returns the same cursor in the
`X-Hatrie-Journal-Next-Cursor` response header. A final page has no next
cursor. With no configured secret, the existing `after_sequence` contract is
unchanged and a cursor request returns `400`.

## Security And Recovery

The cursor is an HMAC-authenticated, bounded token. It contains the last
delivered journal sequence, a schema version, and a 128-bit digest binding it
to the configured journal path. It is not an authentication credential and
must not replace monitoring or replication authentication. Serve it over TLS
when it can reveal journal progress, and treat URLs containing cursors as
sensitive operational data.

Changing the secret or journal path invalidates existing cursors. Journal
compaction still takes precedence: a cursor older than the retained boundary
returns the existing `409` response, so callers must use their normal snapshot
or recovery path. The cursor has no independent expiry; retention and secret
rotation are the invalidation controls.

## Cost Measurement

The focused run used Linux/amd64 on an AMD Ryzen 9 5950X with five 200 ms
samples and `-benchmem`.

| Operation | Median ns/op | B/op | Allocs/op | Wire/token bytes |
| --- | ---: | ---: | ---: | ---: |
| Cursor encode | 856 | 1,040 | 11 | 116 token |
| Cursor decode | 843 | 688 | 10 | n/a |
| JSON tail envelope, cursor disabled | 563 | 272 | 2 | 146 |
| JSON tail envelope, cursor enabled | 691 | 400 | 2 | 279 |

The opt-in cursor adds about 133 JSON bytes and 128 allocated bytes per page in
this small envelope, plus one encode on the producer and one decode on the
consumer. The disabled/default path does not create a codec, token, or extra
field. The 116-byte token is 1.60x smaller than the first implementation's
186-byte token after replacing a 64-byte hex binding with a 16-byte binary
binding.

Reproduce with:

```text
make benchmark-mz024
```
