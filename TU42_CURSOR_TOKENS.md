# T-U42 Cursor Continuation Tokens

`hat/hatPagination` provides an importable Tarantool-inspired `after` token
for ordered cross-request pagination.

```go
codec, _ := hatPagination.NewCodec(secret, hatPagination.DefaultConfig())
token, err := codec.EncodeText("orders", schemaVersion, lastKey, now)
nextKey, err := codec.DecodeText(token, "orders", schemaVersion, now)
```

The token binds the opaque cursor key to a namespace and schema version with
HMAC-SHA256 authentication, supports bounded expiry and key size, rejects
tampering, and has a URL-safe base64 form. Binary tokens are compact for
internal protocols. The caller still owns the strict ordered comparison and
must use the decoded key consistently to guarantee no skipped or duplicated
rows.

The first implementation allocated the HMAC state on every operation. Pooling
the immutable HMAC state and building the token in one buffer reduced binary
encode from about 750 ns/op, 672 B/op, 11 allocations to 307.3 ns/op, 144
B/op, 5 allocations. Binary decode improved from about 698 ns/op, 584 B/op,
10 allocations to 313.0 ns/op, 104 B/op, 5 allocations.

Five-run medians on the local Ryzen 9 5950X:

| Operation | CPU | Wire size | Memory | Allocations |
|---|---:|---:|---:|---:|
| Authenticated binary encode | 307.3 ns/op | 60 B | 144 B/op | 5 |
| Authenticated binary decode | 313.0 ns/op | 60 B | 104 B/op | 5 |
| Authenticated URL-safe encode | 445.2 ns/op | 80 B | 304 B/op | 7 |
| Authenticated URL-safe decode | 405.3 ns/op | 80 B | 168 B/op | 6 |
| Unsigned JSON encode baseline | 272.3 ns/op | 92 B | 160 B/op | 2 |

The JSON row is a serialization-only baseline and does not authenticate,
expire, or bind the cursor to a namespace/version. The secure token therefore
trades a small CPU/allocation cost for tamper resistance and a smaller binary
wire representation.
