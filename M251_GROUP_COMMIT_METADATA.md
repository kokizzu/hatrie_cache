# M251: Reusable Group-Commit Rollback Metadata

`CommandJournal` used a temporary `[]uint32` for encoded record lengths on
every ordinary or idempotent group-commit batch. The lengths are needed only
if a later command is rejected and the suffix must be truncated and
re-appended.

The batch job and idempotent entry now retain their own encoded length. This
keeps rollback behavior unchanged while removing the temporary slice from both
paths. It changes no journal framing, sequence numbers, sync behavior, command
semantics, or public configuration.

## Measurement

Command:

```text
go test ./hat/hatCache -run=^$ -bench BenchmarkM251GroupCommitMetadata -benchmem -count=5
```

Five samples were measured on Linux amd64 with an AMD Ryzen 9 5950X. The
benchmark processes a 64-command group with the write and sync hooks stubbed;
it isolates group-commit bookkeeping rather than filesystem latency.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU | Relative bytes | Relative allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Existing temporary slice | 32,071 | 4,560 | 3 | 1.00x | 1.00x | 1.00x |
| M251 per-job metadata | 32,347 | 4,304 | 2 | 0.99x | 0.94x | 0.67x |

CPU is statistically flat and slightly slower in this sample, so this is not
claimed as a throughput improvement. The retained benefit is one fewer
allocation and 256 fewer allocated bytes per measured batch, with identical
correctness and on-disk behavior. The field is stored in an existing heap
object and does not add a configuration tradeoff.

Raw samples are in [M251_BENCHMARK_RAW.txt](M251_BENCHMARK_RAW.txt).
