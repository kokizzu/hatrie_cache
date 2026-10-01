# Journal-Wide Synchronous Write Quorum

`hat/hatReplication` provides an opt-in
`JournalWriteQuorumCoordinator` for writes that must wait for configured
replicas to apply one exact journal entry before the caller reports success.

This adopts a narrow part of the synchronous write-concern pattern found in
Tarantool and replicated storage engines. It is a caller-owned contract: the
coordinator does not change the existing asynchronous replication path or
discover journal writes automatically.

## Defaults

The zero-value options are disabled:

```go
coordinator, err := hatReplication.NewJournalWriteQuorumCoordinator(
	hatReplication.JournalWriteQuorumOptions{},
)
if err != nil {
	return err
}
```

When disabled, `Wait` returns without invoking the callback, allocates no
result slices, and preserves the existing write behavior. When enabled,
`Required: 0` selects a majority of the configured nodes. Set `Required`
explicitly when a deployment needs a different threshold.

## Exact Acknowledgements

The journal sequence is global to the journal, and the digest identifies the
entry content at that sequence:

```go
coordinator, err := hatReplication.NewJournalWriteQuorumCoordinator(
	hatReplication.JournalWriteQuorumOptions{
		Enabled:  true,
		Nodes:    []string{"east", "west", "backup"},
		Required: 2,
	},
)
if err != nil {
	return err
}

result, err := coordinator.Wait(ctx, hatReplication.JournalWriteQuorumRequest{
	Sequence: journalSequence,
	Digest:   entryDigest,
}, func(ctx context.Context, node string, request hatReplication.JournalWriteQuorumRequest) (hatReplication.JournalWriteQuorumAck, error) {
		return sendAndWaitForExactEntry(ctx, node, request)
	})
if err != nil {
	return err
}
_ = result
```

An acknowledgement counts only when all three conditions hold:

1. the callback returns no error;
2. `Applied` is true; and
3. both `Sequence` and `Digest` exactly match the request.

Every configured target is attempted, even after the threshold is reached, so
`Attempts` contains repair information for failed or stale peers. The
coordinator does not roll back a peer that already applied the entry. The
callback must honor context cancellation and must not report success before the
peer's required durability boundary.

## Errors And Safety

- `ErrJournalWriteQuorumInvalid` covers invalid configuration, nil context,
  zero sequence, and nil callback for an enabled coordinator.
- `ErrJournalWriteQuorumUnsatisfied` is returned when exact acknowledgements
  are below `Required`.
- `ErrJournalWriteQuorumContextCanceled` is returned when cancellation prevents
  the required acknowledgements.
- A stale sequence or digest is recorded as
  `ErrJournalWriteQuorumStaleAcknowledgement` in that attempt and never counts.
- An exact but unapplied response is recorded as
  `ErrJournalWriteQuorumNotApplied` and never counts.

This is not a consensus protocol, membership service, retry policy, or
rollback mechanism. The caller owns peer authentication, journal ordering,
idempotency, retry policy, and repair of attempts that did not acknowledge.
Keep it disabled unless the caller can supply those guarantees.

## Verification

```text
make test-chg11-quorum
make test-chg11-package
make race-chg11-quorum
make vet-chg11-quorum
```

## Benchmark

Linux/amd64 on an AMD Ryzen 9 5950X, five samples with
`-benchtime=100ms -benchmem`:

| Path | Median ns/op | Median B/op | Median allocs/op | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Existing `ExecuteWriteQuorum` three targets | 1,161 | 544 | 10 | Baseline all-target executor |
| Disabled journal coordinator | 4.054 | 0 | 0 | Default no-op overhead |
| Enabled exact journal coordinator | 1,176 | 672 | 9 | Exact sequence/digest safety boundary |

The enabled path is within benchmark noise of the existing executor, uses one
fewer allocation, and retains 128 more bytes per operation for exact attempt
metadata; this run was 1.01x slower than the baseline. This is a safety
capability, not a raw-throughput improvement. The zero-allocation disabled path
is the important default-cost guarantee.
