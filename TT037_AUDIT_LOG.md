# TT-037 Bounded Security Audit Log

`hatAuth.AuditLog` is an opt-in, concurrency-safe, bounded command audit
stream. It assigns monotone sequence numbers, retains the newest events, and
reports how many old events were overwritten. It stores command metadata only:
there is no raw SQL, command payload, token, password, or error field in the
record type.

```go
audit, err := hatAuth.NewAuditLog(4096)
if err != nil {
	return err
}

event, err := audit.Append(hatAuth.AuditRecord{
	Principal: "operator",
	Command:   "SETSTR",
	Namespace: "tenant-eu",
	Resource:  "orders:42",
	Outcome:   hatAuth.AuditOutcomeAllowed,
	Reason:    "policy",
	RequestID: requestID,
	Metadata: []hatAuth.AuditMetadata{
		{Key: "region", Value: "eu"},
		{Key: "authorization", Value: bearerValue}, // stored as <redacted>
	},
})
```

Commands are single tokens, reason values are bounded codes, control bytes are
rejected, and obvious `Bearer` principals are redacted. Metadata keys matching
credential-bearing names such as `authorization`, `password`, `secret`,
`token`, `credential`, or `api_key` always store `<redacted>`. Callers should
still pass identifiers rather than user payloads in namespace, source, and
resource fields.

`Snapshot()` returns the retained window. `Since(sequence, dst)` replays newer
events into a reusable destination and reports whether the requested sequence
was already dropped. The log is bounded in memory but not durable; a caller
that needs persistence should drain `Since` into its own append-only sink.
`NewAuditLog(0)` selects the 1,024-event default. The zero value also lazily
initializes on its first append.

## Measurement

Commands:

```sh
make round78-audit-log-baseline-bench
make round78-audit-log-bench
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X. The baseline is an unsafe raw
fixed-size ring with no validation or synchronization; it is a lower bound,
not a replacement for the security behavior.

| Workload | Baseline median | Audit log median | Relative result |
| --- | ---: | ---: | ---: |
| Plain append | 9.46 ns/op | 184.5 ns/op | 19.5x CPU cost, 0 B/op |
| Append with redacted metadata | not applicable | 252.2 ns/op | 64 B/op, 2 allocs/op |
| Snapshot of 1,024 events | not applicable | 45.9 us/op | 188,418 B/op, 1 alloc/op |

Raw samples (`ns/op`, `B/op`, `allocs/op`):

```text
baseline: 9.533 0 0
baseline: 9.463 0 0
baseline: 9.134 0 0
baseline: 10.46 0 0
baseline: 8.839 0 0
plain: 184.5 0 0
plain: 187.6 0 0
plain: 183.9 0 0
plain: 184.0 0 0
plain: 192.1 0 0
redacted-metadata: 250.6 64 2
redacted-metadata: 252.2 64 2
redacted-metadata: 254.8 64 2
redacted-metadata: 250.5 64 2
redacted-metadata: 254.6 64 2
snapshot: 42990 188418 1
snapshot: 40952 188418 1
snapshot: 45906 188418 1
snapshot: 48364 188418 1
snapshot: 62308 188416 1
```

The audit path is intentionally explicit and opt-in. Its cost is bounded and
allocation-free for ordinary metadata, but it should not be inserted into a
hot data-command loop without a caller-owned sampling or batching policy.
