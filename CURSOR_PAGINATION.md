# Cursor Pagination

`hatDataStructure` provides an authenticated continuation token for ordered
pagination. The token binds a continuation position to an index identity and a
schema version, so a token cannot silently be reused with a different index or
layout.

## Flow

Use one stable secret for the lifetime of the API that issues tokens. Generate
the secret from a secret manager; do not put it in a URL, source file, or
repository. A 32-byte random secret is recommended. Rotating the secret
invalidates tokens issued with the previous secret.

```go
codec, err := hatDataStructure.NewCursorTokenCodec(secret)
if err != nil {
	return err
}

// The key bytes must use the same ordering representation as the index.
token, err := codec.Encode("orders_by_id", schemaVersion, lastKeyBytes, lastID)
if err != nil {
	return err
}

position, err := codec.DecodeFor(token, "orders_by_id", schemaVersion)
if err != nil {
	return err
}

if err := snapshotCursor.SeekAfterEntry(decodeKey(position.Key), position.ID); err != nil {
	return err
}
```

For a live `OrderedIndex`, `SeekAfterEntry` returns an iterator positioned
after the complete `(key, ID)` pair. For an
`OrderedIndexSnapshotCursor`, it updates the stable snapshot cursor in place.
The ID tie-breaker matters when multiple rows have the same ordered key: using
only the key could repeat or skip rows between pages.

## Token Properties

- HMAC-SHA256 authenticates the binary payload.
- Raw URL-safe base64 makes the token suitable for query parameters and HTTP
  headers without JSON escaping.
- `DecodeFor` checks both index identity and schema version before returning a
  position.
- The codec copies the secret and decoded key bytes, avoiding caller-owned
  mutable memory.
- The index name is limited to 256 bytes, the key to 16 KiB, and the encoded
  token to 24 KiB.
- The token is opaque to the API contract but is not confidential: HMAC does
  not encrypt the key or ID. Avoid putting sensitive data in the key.
- The codec does not persist secrets or provide replay expiration. Add an
  application-level expiry or request-scope policy when required.

The token format is deliberately versioned and strictly length-checked. A
malformed, oversized, or truncated token is rejected before its payload is
used. A valid token with a changed payload or secret returns an authentication
error; a valid token used for another index or schema returns a binding
mismatch.

## Benchmark

Local benchmark command:

```text
make benchmark-t-u42
```

Five runs on an AMD Ryzen 9 5950X, Go benchmark `-benchmem`:

| Operation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Encode | 701.5 | 864 | 9 |
| Decode | 677.0 | 648 | 9 |
| DecodeFor | 687.4 | 648 | 9 |

This feature has no previous implementation to compare against. The existing
in-memory cursor seek path remains unchanged; the token allocation cost is
paid only when an application serializes or parses a continuation token.
