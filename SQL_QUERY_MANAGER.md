# SQL Query Manager

`hatSql.SQLQueryManager` is an opt-in operator control modeled after a
kill-query facility. It gives an application a bounded list of active and
completed query IDs, and lets an operator request cooperative cancellation with
a reason.

The ordinary `ExecuteSQLQuery*` functions do not register queries. Existing
callers and defaults therefore pay no manager cost.

## Usage

```go
manager := hatSql.NewSQLQueryManager(256)

result, err := manager.Execute(
    ctx,
    "SELECT id FROM CACHE('items')",
    resolver,
    nil,
    hatSql.QueryOptions{QueryID: "items-report"},
)

status, err := manager.Cancel("items-report", "operator cleanup")
```

`QueryID` is trimmed and limited to 256 bytes. An empty ID receives a bounded
`query-N` ID, returned in `QueryResult.QueryID`. Active IDs are unique. A
non-empty cancellation reason is required and limited to 512 bytes; the first
reason wins when cancellation is requested more than once.

`Status` returns the active status or the most recent retained completed status.
`Active` is sorted by query ID. `History` is oldest-first and bounded by the
capacity passed to `NewSQLQueryManager`; a nonpositive capacity selects the
256-entry default. Completed status stores timestamps, state, cancellation
reason, and a stable error code only. It never stores SQL text, source names,
parameters, or result rows.

## Cancellation Semantics

Cancellation is cooperative and uses the same `context.Context` checks as the
SQL executor. A resolver that blocks outside the executor must honor its own
context or return; the manager cannot interrupt arbitrary user code. Once a
cancel request is accepted before completion is published, the manager reports
`SQLQueryStateCanceled` even if a resolver returns a successful value after the
context was canceled.

Operator cancellation returns `*SQLQueryCanceledError`. It includes the query ID
and bounded reason while still satisfying `errors.Is(err, context.Canceled)`.
Context deadlines are reported as canceled status with the existing
`context.DeadlineExceeded` error chain. Duplicate IDs return the existing
`ErrorConflict` classification.

This API is a library control surface. An HTTP, gRPC, or CLI layer must apply
its existing authentication and authorization policy before exposing
`Cancel`, `Active`, or `History` to remote operators.

## Cost

The manager is intentionally opt-in. On the repository's AMD Ryzen 9 5950X
benchmark fixture, a one-row query measured a median 3,169 ns/op, 3,920 B/op,
and 27 allocations directly, versus 4,075 ns/op, 4,175 B/op, and 32
allocations through a manager. That is about 1.29x CPU, 1.07x allocated bytes,
and 1.19x allocations for bounded operator control. The completed-status ring
does not shift old entries on each query. Reproduce with:

```text
make benchmark-sql-query-manager
```
