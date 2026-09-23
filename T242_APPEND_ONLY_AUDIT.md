# T242 Append-Only Audit Coverage

`hatCache.MonitoringOptions.AuditAllOperations` enables complete HTTP monitoring
request coverage when an `AuditLog` is configured. It is disabled by default.

```go
auditLog, err := hatCache.OpenAuditLogger("/var/lib/hatrie/audit.jsonl")
if err != nil {
	return err
}
defer auditLog.Close()

handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
	NodeName:           "cache-a",
	AuditLog:           auditLog,
	AuditAllOperations: true,
})
```

The file logger opens the path with append-only writes and mode `0600`. The
logger serializes concurrent writes and retains a bounded recent window for
the audit API. It does not `fsync` every event; use a durable filesystem and
close the logger during orderly shutdown when crash-loss bounds matter.

## Event Semantics

The option emits one `http.request` event for every monitoring request that
does not already emit a detailed event. Existing command, SQL, backup,
storage, replication, journal, and profile events remain the source of truth
for those operations, so enabling the option does not duplicate them.

Generic events include the node, protocol, remote address, method, URL path,
HTTP status, success flag, and status text. The query string, request headers,
and request body are intentionally excluded. Unauthorized requests are also
recorded when the audit logger is configured, which makes rejected operator
activity visible without recording bearer tokens.

Use `NewRedactedAuditLogger` or `AuditLoggerOptions.RedactSensitive` when
detailed command or SQL events may contain sensitive keys or metadata.

## Cost

The default remains unchanged and pays no generic-audit wrapper cost. The
opt-in path adds response tracking, JSONL encoding, and one synchronized log
write per otherwise-uninstrumented request.

Measured with:

```text
make t242-benchmark
```

One run on the test host produced:

| Path | ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Audit disabled | 2,778 | 1,412 | 16 | 1.00x |
| AuditAllOperations enabled | 5,324 | 2,712 | 24 | 1.92x slower |

The opt-in path therefore adds about `2,546 ns/op`, `1,300 B/op`, and 8
allocations for this small health/config-style request. Treat these numbers as
host- and workload-dependent; rerun the benchmark for production sizing.

## Verification

Focused correctness, race, and static checks:

```text
make t242-test
make t242-race
make t242-vet
```

The focused tests cover default-off behavior, generic success records,
detailed-event de-duplication, unauthorized requests, and query/secret
non-leakage. The full `hatCache` package command was also run; it currently
fails in pre-existing SQL planner expectation tests unrelated to this audit
change. The focused T242 tests pass.
