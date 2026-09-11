# Persistent SQL Query Log

`SQLQueryManager` already retains a bounded in-memory status history. The
CH-031 addition provides an opt-in durable counterpart inspired by
ClickHouse's query log: terminal query status is appended as newline-delimited
JSON so operators can inspect history after a process restart.

## Usage

```go
log, err := hatriecache.OpenSQLQueryLog("var/lib/hatrie/query-log.ndjson")
if err != nil {
	return err
}
defer log.Close()

manager := hatriecache.NewSQLQueryManagerWithOptions(hatriecache.SQLQueryManagerOptions{
	HistoryCapacity: 256,
	QueryLog:        log,
})
_, err = manager.Execute(ctx, query, resolver, parameters, hatriecache.SQLQueryOptions{
	QueryID: "request-123",
})
if logErr := manager.QueryLogError(); logErr != nil {
	// The query result is still valid; alert or retry the observability write.
	return logErr
}
if err := log.Sync(); err != nil {
	return err
}
```

`OpenSQLQueryLogWithOptions` accepts `SyncOnAppend: true` when every completed
query must issue a filesystem sync. The default is `false`: writes are
serialized and appended immediately, while `Sync` or `Close` establishes the
durability checkpoint. A failed optional log write is exposed through
`QueryLogError` and does not convert a successful query into a failed query.

## Record And Security Contract

Each record contains `query_id`, terminal `state`, start and finish timestamps,
duration in nanoseconds, and the machine-readable error code when present. It
does not contain SQL text, source names, parameters, result rows, or operator
cancellation reasons. The file is created with mode `0600`; missing parent
directories use mode `0700`, and symlink log paths are rejected. Reads reject
malformed, oversized, truncated, non-terminal, or timestamp-invalid records.

The log is append-only NDJSON and can be processed with standard line-oriented
tools after access control is applied. Rotation and retention should be handled
by deployment policy; the API does not delete or rewrite historical records.

## Measured Tradeoff

Five samples used the repository benchmark target on an AMD Ryzen 9 5950X.
The manager comparison uses the same simple query and measures the default
path before and after adding the nil log check. Direct append measurements
show the cost of opting into persistence.

| Workload | Before | After | Result |
| --- | ---: | ---: | --- |
| Default manager completion | 3,611 ns/op; 3,968 B; 29 allocs | 3,576 ns/op; 3,968 B; 29 allocs | 1.01x faster; no memory change |
| Buffered log append | not applicable | 2,349 ns/op; 369 B; 4 allocs | opt-in write cost |
| Sync-on-append log append | not applicable | 646,927 ns/op; 374 B; 4 allocs | about 275x slower than buffered append |

The durable feature is therefore opt-in. The default SQL manager keeps the
prior allocation profile, while operators can choose buffered throughput or
per-record crash durability explicitly.

## Verification

```text
make test-ch031-persistent-query-log
make benchmark-ch031-baseline
make benchmark-ch031-persistent-query-log
```
