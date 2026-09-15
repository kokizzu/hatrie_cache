# CH-U07 Mutation Lifecycle

Hatrie Cache now exposes a ClickHouse-style mutation lifecycle for asynchronous
journal writes. A caller receives a repeatable submission handle, waits for the
durable-and-applied point, reads the assigned journal sequence, and polls the
terminal outcome without retaining a second history map.

## API

```go
submission, err := journal.SubmitAsyncCommand(trie, hatCache.CacheCommandRequest{
    Command: "SETSTR",
    Key:     "customer:42",
    Value:   "active",
})
if err != nil {
    return err
}

response, err := submission.Wait(ctx)
sequence := submission.Sequence()
state := submission.Status()
infraErr := submission.Error()
```

`Sequence()` is zero while queued and for a command that is rejected or fails
before a durable record is retained. A successful submission receives the
journal sequence after the batch is fsynced and before the command is applied.
`Status()` distinguishes `pending`, `committed`, `rejected`, and `failed`;
`AsyncCommandSubmissionCompleted` remains a compatibility alias for the
committed state. `Error()` reports infrastructure failure, while a command
rejection remains in the `CacheCommandResponse` returned by `Wait`.

The durable status can be checked after restart:

```go
status, err := reopenedJournal.MutationStatus(sequence)
// status.State == hatCache.CommandJournalMutationCommitted
// status.Progress == 1
```

`MutationStatus` rereads one framed journal record by sequence. It is
idempotent and does not allocate a historical operation map. A sequence that
was compacted or never committed returns the existing journal error or
`ErrCommandJournalMutationNotFound`.

## SQL And Privacy

`CACHE('system.mutations')` retains its existing `sequence`, `command`, `key`,
and `state` fields and adds `mutation_id` and integer `progress`. The table
never exposes the command value or binary payload. It reports durable committed
records; live rejected and failed attempts are available through their
submission handle and are not written as successful journal mutations.

## Tradeoff

The live handle adds a small mutex-protected sequence/status/error snapshot but
does not retain a global map. On the local AMD Ryzen 9 5950X host,
`BenchmarkCHU07SubmissionMetadata` measured 10.93--11.71 ns/op, `0 B/op`, and
`0 allocs/op`. `MutationStatus` measured 11.70--12.72 us/op, 4,944 B/op, and
14 allocs/op across five runs; the direct `Tail(0, 1)` comparator measured
11.43--12.52 us/op, 4,920 B/op, and 14 allocs/op. The roughly 0.2 us wrapper
cost buys sequence validation and a typed lifecycle result. It is intended for
operator polling, not for replacing a high-throughput journal tail stream.

The one-shot status lookup process used 51,816 KiB maximum RSS after compiling
the test binary separately. The lookup itself is bounded by one journal record;
the measured RSS includes Go runtime and test-binary overhead.

## Verification

The tests cover successful async commit, restart lookup, SQL metadata, command
rejection without a phantom sequence, and injected fsync failure with rollback.
Run:

```text
make test-chu07-c247
make test-chu07-package-c247
make race-chu07-c247
make vet-chu07-c247
make benchmark-chu07-c247
make memory-chu07-c247
```
