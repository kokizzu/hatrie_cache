# Materialize-style File Command-Journal Sink

`FileCommandJournalExactlyOnceSink` is a concrete local connector for the
existing `CommandJournalExactlyOnceSink` runner. It is useful when a durable
filesystem handoff is needed without introducing an external broker.

## Usage

```go
sink, err := hatCache.NewFileCommandJournalExactlyOnceSink(
    "/var/lib/hatrie/command-sink",
)
if err != nil {
    return err
}

runner, err := journal.StartCommandJournalExactlyOnceSink(
    ctx, // service-owned context; cancellation stops the runner
    sink,
    hatCache.CommandJournalExactlyOnceSinkOptions{BatchSize: 100},
)
if err != nil {
    return err
}
return runner.Wait()
```

The sink creates one immutable `batch-<last-sequence>.hjs` file per committed
batch. `Begin` requires the caller's watermark to equal the highest validated
committed sequence, and `Write` requires the first record to be the immediate
next sequence. This prevents concurrent writers, gaps, and stale retries from
silently advancing the output.

## Durability and Recovery

Each HJS1 file contains first/last sequence metadata, record count, a bounded
JSON payload, and a CRC32C checksum. Commit writes a `0600` temporary file,
flushes it, and atomically renames it into the connector directory. The
directory is created with mode `0750`. Missing directories are empty sinks;
any malformed, overlapping, or checksum-invalid committed batch makes load
fail rather than hiding corruption behind a newer file.

The existing exactly-once runner treats a commit error as an unknown outcome
and stops, as required by its interface. Reopen the sink and inspect
`LoadSequence`/`ReadBatches` before deciding whether an external retry is safe.
The connector is append-only: retention or compaction of old batch files must
be coordinated with the downstream consumer and upstream journal retention.
No automatic deletion is performed.

`MaxBatchBytes` defaults to 8 MiB and is hard-capped at 64 MiB. The bound
covers the complete encoded file, including metadata and checksum.

## Measured Tradeoff

AMD Ryzen 9 5950X, Linux, Go benchmark harness. Five samples were collected;
the existing runner benchmark used `-benchtime=100x`, and file benchmarks used
`-benchtime=20x` because they perform real filesystem sync and rename work.

| Operation | Median ns/op | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Existing in-memory exactly-once runner, 100 records | 153,216 | 167,301 | 536 |
| File sink commit, 100 records | 2,052,795 | 791,321 | 349 |
| File sink read, 100 records | 119,460 | 78,655 | 39 |

The durable commit is about 13.4x slower than the in-memory runner batch and
uses about 4.7x the transient bytes. That is the cost of persistence, not a
regression to the existing default path: the connector is opt-in and only
used when a filesystem handoff is wanted.

## Verification

```text
make test-mz011-file-sink
make race-mz011-file-sink
make vet-mz011-file-sink
make benchmark-mz011-file-sink
```

The full `hat/hatCache` package target should also be run when the parallel
SQL planner worktree is green:

```text
make test-mz011-file-sink-package
```
