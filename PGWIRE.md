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

## Prepared Queries

The server supports the PostgreSQL extended-query sequence `Parse`, `Bind`,
`Describe`, `Execute`, `Close`, `Flush`, and `Sync`. Parameters remain separate
from SQL text and are forwarded to `hatSql` as positional `$1`, `$2`, and later
values. Closing a statement or portal removes its connection-local state. This
is the prepared-statement baseline required by JDBC and ODBC clients.

`Bind` supports PostgreSQL text-format values and SQL NULL. Declared `bool`,
`int2`, `int4`, `int8`, `float4`, and `float8` parameters are converted to Go
values before Hatrie SQL execution; text and unspecified OIDs stay strings.
Results are sent in PostgreSQL text format. Binary bind values or binary result
formats receive SQLSTATE `0A000` rather than being interpreted incorrectly.

`Describe Statement` returns the declared parameter OIDs; `Describe Portal`
returns `NoData` until result-schema inference is available. The package does
not terminate TLS, so use a TLS proxy or a protected local listener; an SSL
request receives `N` to permit `sslmode=prefer` fallback.

`Execute` accepts a nonzero row limit for cursor-style fetching. The result is
materialized once per portal, each execute returns its next row range, and the
server emits `PortalSuspended` until all rows have been returned.

## Embed A Listener

```go
listener, err := net.Listen("tcp", "127.0.0.1:5433")
if err != nil {
	return err
}
handler := hatSql.NewPgWireQueryHandler(sqlResolver, hatSql.QueryOptions{})
for {
	connection, err := listener.Accept()
	if err != nil {
	return err
	}
	go func() {
		defer connection.Close()
		_ = hatPgWire.ServeConn(context.Background(), connection, handler, hatPgWire.ServerOptions{
			Authenticator: verifyPassword,
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
