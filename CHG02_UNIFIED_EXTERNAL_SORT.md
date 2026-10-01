# CH-G02 Unified External Sort: Star Projections

This progress unit extends the existing opt-in external `ORDER BY` path to
direct `CACHE` and `VALUES` queries whose projection is `SELECT *` (including a
qualified star). The sorter still uses the existing spill lifecycle and keeps
the default in-memory execution path unchanged.

## Configuration

External sorting remains disabled unless all three options are set:

```go
hatSql.QueryOptions{
	MaxSortBytes:   16 << 10,
	SpillDirectory: "/var/lib/hatrie-cache/spill",
	MaxSpillBytes:  64 << 20,
}
```

The implementation sorts a bounded run, writes it to the configured spill
directory, merges the runs, and removes every temporary file on success,
quota failure, cancellation, or callback failure. Star columns are emitted in
deterministic lexical order because schema-less `SQLRow` values are maps.
Mixed `SELECT *, expression` projections remain on the existing path until a
stable output-column contract is available.

## Verification

Focused tests cover:

- streamed `CACHE('events') SELECT * ORDER BY ...` output and column order;
- local `VALUES ... SELECT * ORDER BY ...` output;
- source materialization is not called for the streamed `CACHE` path;
- spill-file cleanup after callback cancellation.

Commands:

```text
make format-chg02-star-spill
make test-chg02-star-spill
make race-chg02-star-spill
make vet-chg02-star-spill
make benchmark-chg02-star-spill
```

## Benchmark

Workload: 2,048 rows, two columns, descending `id`, five benchmark samples on
an AMD Ryzen 9 5950X. The baseline is the existing materialized `SELECT *`
path. The new path is explicit disk-backed spill with `MaxSortBytes=16 KiB`.

| Path | Median ns/op | B/op | allocs/op | Result |
|---|---:|---:|---:|---|
| Before, materialized baseline | 2,745,166 | 1,319,164 | 8,220 | succeeds |
| After, materialized baseline | 2,756,041 | 1,319,166 | 8,220 | unchanged default path |
| After, streaming spill | 13,824,965 | 4,160,701 | 87,286 | succeeds under the explicit spill budget |

The spill path is intentionally slower and has higher cumulative allocation
because it serializes and merges temporary runs. That is the cost of turning
the previous streamability error into a bounded-memory result. It is not the
default, and the default benchmark stayed within normal run-to-run noise.
