# PostgreSQL Wire Protocol

`hat/hatPgWire` provides a dependency-free PostgreSQL v3 simple-query server
for existing PostgreSQL client libraries. It performs startup negotiation,
answers an SSL negotiation request with `N` so `sslmode=prefer` clients can
fall back to plain TCP, supports optional clear-text password authentication,
and sends text-format rows, SQL NULLs, command completion, and PostgreSQL error
responses.

The wire transport executes the Hatrie SQL dialect, not the full PostgreSQL SQL
dialect. PostgreSQL clients such as `psql`, BI connectors, or driver-based
tools must submit syntax supported by [SQL.md](SQL.md). Extended prepared-query
messages are intentionally rejected with SQLSTATE `0A000`; they are a separate
compatibility milestone required for JDBC and ODBC driver support.

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
