# CH-G02 Unified External Sort

CH-G02 adds `hat/hatSort`, an importable, stable external-sort primitive for
workloads that cannot keep their complete sort state in memory. It sorts
opaque `Key`/`Value` records, spills bounded sorted runs to a caller-selected
directory, merges them with a bounded fan-in, and removes temporary files on
success, cancellation, callback failure, and spill-budget failure.

The package is deliberately opt-in. A normal in-memory stable sort is faster
and uses less memory when the input fits the available budget. SQL wiring is
not changed by this feature; callers can use `ExternalSortInto` at an explicit
spill boundary without changing existing query semantics.

## API

```go
stats, err := hatSort.ExternalSortInto(ctx, records, hatSort.Options{
    MaxMemoryBytes: 64 << 20,
    MaxSpillBytes:  8 << 30,
    MaxMergeRuns:   32,
    SpillDirectory: "/var/lib/hatrie/tmp/sort",
}, func(record hatSort.Record) error {
    return writeOutput(record.Key, record.Value)
})
```

`ExternalSort` is the convenience form that returns a complete slice. Use
`ExternalSortInto` when the consumer can process rows incrementally; callback
byte slices are borrowed until the callback returns and must be copied if the
consumer retains them.

Zero-valued limits select these defaults:

| Option | Default | Purpose |
| --- | ---: | --- |
| `MaxMemoryBytes` | 4 MiB | In-memory run buffer and maximum individual record size |
| `MaxSpillBytes` | 1 GiB | Cumulative temporary-run bytes written during the sort |
| `MaxMergeRuns` | 32 | Maximum open runs per merge pass |

The default comparator uses bytewise key order. A custom comparator is
available for typed encodings. Equal keys preserve input order across run
creation and merge passes.

## Temporary storage and recovery

If `SpillDirectory` is empty, the package creates a private temporary
directory and removes it before returning. If it is set, the directory itself
is retained but all package-owned `.hatrie-sort-*` files are removed. Run files
use a binary length-prefixed format with 32-bit key and value lengths; malformed
or truncated files return `ErrCorruptRun`. The package never executes or
deserializes code from a run file.

`MaxSpillBytes` is a cumulative I/O guard, including intermediate merge output,
not a promise about peak disk occupancy. Applications should provision the
spill directory separately and use a dedicated filesystem or quota.

## Measurement

Commands:

```sh
make chg02-external-sort-test
make chg02-external-sort-race
make chg02-external-sort-vet
make chg02-external-sort-benchmark
```

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X, sorting 8,192
records. The in-memory baseline is the existing `sort.SliceStable` path. The
pre-optimization samples were collected before `ExternalSort` stopped copying
already detached output records a second time.

| Workload | Before median ns/op | After median ns/op | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Existing in-memory stable sort | 4,218,934 | 4,206,448 | 524,411 | 524,411 | 16,388 | 16,388 |
| CH-G02 no spill, slice result | 5,742,348 | 5,327,228 | 2,645,988 | 2,514,917 | 32,790 | 16,406 |
| CH-G02 spill, slice result | 8,856,800 | 8,347,878 | 3,686,310 | 3,555,232 | 99,137 | 82,753 |
| CH-G02 spill, streaming result | 8,027,816 | 8,094,743 | 3,165,413 | 3,165,407 | 82,752 | 82,752 |

The second-copy removal improves the slice API by approximately 1.08x in the
no-spill median, 1.06x in spill median, 2.00x fewer no-spill allocations, and
1.20x fewer spill allocations. The streaming API remains the preferred form
for bounded retained memory. It is not faster than an in-memory sort because
temporary-file writes, reads, and merge comparisons are real costs; its value
is bounded memory for inputs that otherwise cannot complete.

Peak run-buffer memory in the spill benchmark was 4,080 bytes per operation,
with 35 initial runs and 278,528 cumulative temporary bytes for the measured
input. These counters describe the sorter’s bounded run state and I/O work,
not total process RSS.

## Safety checks

Focused tests cover stable ordering, input/output ownership, cancellation,
record and spill limits, bounded merge passes, streaming output, and cleanup
after both success and failure. Race detection and `go vet` are part of the
feature targets.
