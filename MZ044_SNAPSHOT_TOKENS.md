# MZ-044 Session Snapshot Tokens

`hatSql` now supports an opt-in signed token that carries one immutable source
frontier across multiple SQL queries. It packages the existing
`SQLFrontierSnapshotProvider` and `AsOfFrontier` behavior; it does not add a
second snapshot implementation or retain server-side session state.

## Use

Create one codec per service or trust domain and keep its secret private:

```go
codec, err := hatSql.NewSQLSnapshotTokenCodec(hatSql.SQLSnapshotTokenCodecOptions{
	Secret: os.Getenv("HATRIE_SQL_SNAPSHOT_TOKEN_SECRET"),
})
if err != nil {
	return err
}

token, err := codec.Encode(frontier)
if err != nil {
	return err
}

result, err := hatSql.ExecuteSQLQueryParameters(ctx, query, resolver, params,
	hatSql.SQLQueryOptions{
		SnapshotToken:      token,
		SnapshotTokenCodec: codec,
	})
```

The same token can be supplied to each related query. The resolver receives
the decoded frontier through its existing `BeginSQLSnapshotAt` method, so it
decides whether that historical view is still available.

## Defaults And Limits

- `SnapshotToken` is empty by default, so existing queries do not decode a
  token and do not pay token overhead.
- A zero `MaxAge` uses `DefaultSQLSnapshotTokenMaxAge` (15 minutes).
- Secrets shorter than 16 bytes are rejected.
- The token is 53 raw bytes and 71 URL-safe base64 characters.
- An explicit `AsOfFrontier` and `SnapshotToken` cannot be combined.
- A resolver without frontier snapshot support returns the existing
  `ErrSQLAsOfUnsupported` error after token validation.

## Security

The payload is versioned binary data containing an issue timestamp and a
`uint64` frontier, authenticated with HMAC-SHA256. Decode verifies the MAC
before trusting either field, rejects malformed or future-dated tokens, and
rejects tokens older than `MaxAge`.

The token is not an authorization credential. The caller must still pass the
service's normal authentication and authorization checks, and the resolver
must enforce source and frontier access. Rotate the secret by deploying a new
codec configuration; already issued tokens signed by the old secret then stop
verifying.

## Tradeoff

Token creation and validation add work only when this feature is selected.
The query path reuses the existing frontier provider, but token decoding adds
CPU, heap traffic, and allocations. See the MZ-044 section in
`BENCHMARK.md`; the default path is unchanged and remains the lower-cost
choice when cross-query consistency is unnecessary.
