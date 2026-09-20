# T-U05: Session Transaction Settings

`hatSql.SQLSession` now exposes one validated transaction-settings contract for
session callers. The feature is opt-in and preserves the old behavior when a
session is created with its defaults.

```go
session := hatSql.NewSQLSession(source)
if err := session.SetTransactionSettings(hatSql.SQLSessionTransactionSettings{
	Isolation:  hatSql.SQLSessionIsolationRepeatableRead,
	Timeout:    5 * time.Second,
	ReadOnly:   true,
	Durability: hatSql.SQLSessionDurabilityDurable,
}); err != nil {
	return err
}
settings := session.TransactionSettings()
```

## Defaults and behavior

The default contract is read-write, `read_committed`, in-memory durability, and
no session-imposed timeout. A zero isolation or durability value is normalized
to those defaults. Timeouts are bounded to 24 hours; zero leaves the caller's
context unchanged. A parent context deadline remains authoritative.

`ReadOnly` rejects session mutations through temporary-table creation and named
results, views, projections, and transactional view batches. Direct temporary
table drops become no-ops in read-only mode because the legacy method has no
error return. Ordinary query reads remain available.

Isolation and durability are validated and exposed as metadata for the
caller-owned transaction/commit engine. `SQLSession` is an in-memory query
session and does not add MVCC, external-source locking, fsync, or a durable
commit protocol by itself.

`ResetTransactionSettings` restores the defaults. Settings replacement is
atomic and snapshots are copied, so concurrent readers never observe a partial
configuration.

## Cost

Linux/amd64, AMD Ryzen 9 5950X, Go `-benchmem`, five samples. The query is a
small `FROM VALUES` session query.

| Path | Median ns/op | B/op | allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Before T-U05 | 7,215 | 5,328 | 33 | baseline |
| After, default settings | 7,105 | 5,328 | 33 | allocation-neutral; CPU within run noise |
| After, 1-minute timeout | 7,667 | 5,600 | 37 | opt-in timeout bookkeeping: 1.08x CPU and +272 B/+4 allocs vs final default run |

The default path does not create a derived context. The timeout cost is paid
only by sessions that request a timeout and is the necessary context deadline
allocation rather than a process-wide overhead.

## Verification

```text
make test-tu05-session-settings
make race-tu05-session-settings
make vet-tu05-session-settings
make benchmark-tu05-after
```

The full `hatSql` package was also run; three unrelated typed-table arrangement
checkpoint tests remain failing on this isolated base. They do not exercise the
T-U05 code or tests.
