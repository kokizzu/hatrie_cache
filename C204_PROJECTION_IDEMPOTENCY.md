# C204: Projection Idempotency Metadata

Hatrie Cache already uses the durable command journal's idempotency key to
deduplicate retrying async writes. C204 carries that identity through the
SQL incremental-projection path so operators can correlate a source write
with the materialized views refreshed by it.

## Behavior

`SQLJournalProjectionRunner` reads the idempotency key from each journal
entry and passes it to
`hatSql.IncrementalProjectionRunner.ApplyWithIdempotencyKeys`. A successful
projection run exposes the non-empty keys in source sequence order through
`ProjectionRun.IdempotencyKeys`. Each refreshed materialized view exposes the
same batch metadata through `MaterializedViewStatus.IdempotencyKeys`.

The existing APIs remain compatible:

- `Apply` and `RefreshChanged` keep their previous behavior.
- Calls without keys do not add the metadata field to JSON output.
- Rebuilds and replayed changes at or below the checkpoint do not invent
  identity metadata.

The metadata is correlation information, not a second deduplication system.
The journal sequence and checkpoint remain the correctness boundary, and the
source journal remains responsible for idempotency enforcement.

## Example

```go
run, err := runner.ApplyWithIdempotencyKeys(ctx, changes, []string{"write-42"})
if err != nil {
	return err
}
fmt.Println(run.IdempotencyKeys)

status, ok := views.Get("people_view")
if ok {
	fmt.Println(status.Status.IdempotencyKeys)
}
```

Keys are trimmed and empty values are ignored. The projection runner requires
the supplied key slice to have the same length as the change slice; this
keeps sequence-to-identity alignment explicit.

## Cost

The legacy projection path remains the default and has no additional
allocation for absent metadata. The focused benchmark in
[`BENCHMARK.md`](BENCHMARK.md#c204-projection-idempotency-metadata) measured
the opt-in metadata path at roughly `1.05x` the CPU time, `+80 B/op`, and `+5
allocs/op`. The feature should therefore be enabled when correlation is
useful, rather than as a replacement for the journal's existing idempotency
checks.
