# PostgreSQL Wire Protocol

`hat/hatPgWire` provides a dependency-free PostgreSQL v3 query server
for existing PostgreSQL client libraries. It performs startup negotiation,
answers an SSL negotiation request with `N` so `sslmode=prefer` clients can
fall back to plain TCP, supports optional clear-text password authentication,
and sends text-format rows, SQL NULLs, command completion, and PostgreSQL error
responses.

The wire transport executes the Hatrie SQL dialect, not the full PostgreSQL SQL
dialect. PostgreSQL clients such as `psql`, BI connectors, or driver-based
tools must submit syntax supported by [SQL.md](SQL.md).

PostgreSQL `SET` session setup statements are accepted as no-ops for client
initialization compatibility; Hatrie does not persist PostgreSQL session
settings between queries.

## Prepared Queries

The server supports the PostgreSQL extended-query sequence `Parse`, `Bind`,
`Describe`, `Execute`, `Close`, `Flush`, and `Sync`. Parameters remain separate
from SQL text and are forwarded to `hatSql` as positional `$1`, `$2`, and later
values. Closing a statement or portal removes its connection-local state. This
is the prepared-statement baseline required by JDBC and ODBC clients.

`Describe Portal` emits the portal's `RowDescription`. Its later `Execute`
emits only `DataRow` messages followed by `PortalSuspended` or
`CommandComplete`; it never repeats the row description. This ordering matches
the PostgreSQL v3 protocol and is covered by the local pgJDBC prepared-query
integration test.

After an error in `Parse`, `Bind`, `Describe`, `Execute`, `Close`, or another
extended-query message, the server emits one `ErrorResponse`, discards later
extended-query messages, and emits one `ReadyForQuery` only when it receives
`Sync`. Simple-query errors remain immediately followed by `ReadyForQuery`.

`Bind` supports PostgreSQL text-format values and SQL NULL. Declared `bool`,
`int2`, `int4`, `int8`, `float4`, and `float8` parameters are converted to Go
values before Hatrie SQL execution; text and unspecified OIDs stay strings.
Results are sent in PostgreSQL text format. Binary bind values or binary result
formats receive SQLSTATE `0A000` rather than being interpreted incorrectly.

`Describe Statement` returns the declared parameter OIDs. `Describe Portal`
materializes and caches the portal result to return its row description, so the
later `Execute` reuses the same result. The package does not terminate TLS, so
use a TLS proxy or a protected local listener; an SSL request receives `N` to
permit `sslmode=prefer` fallback.

`Execute` accepts a nonzero row limit for cursor-style fetching. The result is
materialized once per portal, each execute returns its next row range, and the
server emits `PortalSuspended` until all rows have been returned.

`ServerOptions.MaxPreparedStatements` optionally bounds connection-local named
and unnamed prepared statements. The default is unlimited for compatibility;
set a finite value on network-facing listeners to limit retained query state.
`ServerOptions.MaxPortals` similarly bounds bound portals and their retained
parameter vectors or materialized results.
`ServerOptions.MaxPortalResultBytes` bounds the encoded field-name and text-cell
payload retained by one materialized portal result.

PostgreSQL wire cancellation is disabled by default. Create one
`hatPgWire.CancelRegistry` per listener and pass that same registry through
`ServerOptions.CancelRegistry` for every accepted connection. The server then
publishes registered `BackendKeyData`; a second connection carrying a matching
PostgreSQL `CancelRequest` cancels only the active handler call and returns
SQLSTATE `57014`. A mismatched or expired key does nothing. Query handlers must
honor their context for cancellation to take effect promptly.

## Compatibility Check

`make test-pgwire-server` includes loopback integration tests with the stock
`psql` client and pgJDBC prepared statements when their local tools are
available. The pgJDBC test uses the non-versioned local cache
`$TMPDIR/hatrie_cache_pgwire_jdbc/postgresql-42.7.5.jar`; it skips when that
driver is absent and never downloads a dependency. Run
`make check-pgwire-client-tools` to report local `psql`, Java/JDBC, and ODBC
client availability.

## Embed A Listener

```go
listener, err := net.Listen("tcp", "127.0.0.1:5433")
if err != nil {
	return err
}
handler := hatSql.NewPgWireQueryHandler(sqlResolver, hatSql.QueryOptions{})
cancelRegistry := hatPgWire.NewCancelRegistry()
for {
	connection, err := listener.Accept()
	if err != nil {
	return err
	}
	go func() {
		defer connection.Close()
		_ = hatPgWire.ServeConn(context.Background(), connection, handler, hatPgWire.ServerOptions{
			Authenticator: verifyPassword,
			CancelRegistry: cancelRegistry,
		})
	}()
}
```

`Authenticator` receives the startup user/database parameters and the password.
Set it for every network-reachable listener. Leaving it nil enables PostgreSQL
trust authentication and is suitable only behind a protected local, mTLS, or
trusted-proxy boundary. The protocol package does not terminate TLS; put TLS in
front of the listener or use the existing monitoring TLS boundary.

The `hatSql` adapter infers common PostgreSQL OIDs for boolean, integral,
floating-point, timestamp, and text values. Results use PostgreSQL text format;
this preserves compatibility while Apache Arrow IPC remains the intended
columnar analytics path.
