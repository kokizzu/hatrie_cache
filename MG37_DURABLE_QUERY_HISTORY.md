# M-G37: Durable Redacted Query History

`hat/hatQueryHistory` is an importable, opt-in durable history sink for
completed-query status. It borrows the operational value of ClickHouse query
logs and Materialize query history while keeping the persisted schema
deliberately redacted.

## Persisted Contract

Each JSONL record contains only:

- an application-supplied bounded query ID;
- lifecycle state;
- start and finish timestamps;
- a bounded error code.

There is no field for SQL text, parameters, result rows, or cancellation
reasons. The active file is created with mode `0600`; the loader rejects an
unknown format, malformed JSON, invalid state, control characters, or entries
over the configured bound.

The current file rotates to `<path>.1` when its byte limit is reached. On
reopen, the rotated file is read before the current file and only the latest
`MaxEntries` records are retained in memory. The registry itself is bounded;
it never grows with unbounded history.

## Defaults

| Limit | Default |
| --- | ---: |
| Retained records | 1,024 |
| Encoded record | 4 KiB |
| Active file | 16 MiB |
| File permissions | `0600` |
| Per-append fsync | off |

An explicit path is required. `Sync: true` provides stronger crash durability
at a substantial latency cost. Existing SQL query-manager behavior is
unchanged; callers can map its privacy-safe terminal status fields into
`QueryRecord` and append them at the completed-query boundary.

## Example

```go
history, err := hatQueryHistory.Open(hatQueryHistory.Options{
	Path: "/var/lib/hatrie/query-history.jsonl",
	Sync: true,
})
if err != nil {
	return err
}
defer history.Close()

err = history.Append(hatQueryHistory.QueryRecord{
	QueryID:    "query-42",
	State:      hatQueryHistory.StateSucceeded,
	StartedAt:  startedAt,
	FinishedAt: finishedAt,
})
```

Use `Snapshot` for bounded diagnostics and `Close` during shutdown. Do not put
SQL or secret material into `QueryID` or `ErrorCode`; those fields are
bounded and control-character-safe, but they are still caller-provided text.

## Measured Tradeoff

The clean-base SQL package cannot supply a valid pre-change benchmark because
its existing `hatSql` build fails on unrelated missing
`MaxDataflowTextBytes`, `TypedTableDate`, and `TypedTableTimestamp` symbols.
The feature benchmark therefore uses an in-memory ring append as the lower
bound, then measures durable append and snapshot paths directly.

Five samples were collected on AMD Ryzen 9 5950X, linux/amd64, with
`go test -run '^$' -bench '^BenchmarkMG37' -benchmem -count=5`.

Raw samples in `ns/op`:

```text
InMemoryAppendBaseline  2.476  2.600  2.482  2.303  2.481
DurableAppend        2247    2278    2251    2233    2236
DurableAppendSync  685408 650361 691529 668901 645628
DurableSnapshot     21020  22642  21762  26380  23351
```

| Operation | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| In-memory append baseline | 2.481 | 0 | 0 | 1.00x |
| Durable append, no fsync | 2,247 | 369 | 4 | 906x |
| Durable append, `Sync: true` | 668,901 | 374 | 4 | 269,609x |
| Snapshot of 1,024 records | 22,642 | 98,304 | 1 | not comparable |

This is an observability/durability feature, not a performance optimization.
It is acceptable only when enabled deliberately at query-completion or audit
boundaries; it must not be inserted into per-row execution. Keep `Sync` off
when throughput matters and the operator can tolerate the filesystem's normal
write-back window.

## Verification

Focused tests cover persistence and reopen, redaction, file permissions,
rotation, entry and history bounds, corrupt-file rejection, idempotent close,
concurrent appends, race detection, and `go vet`.
