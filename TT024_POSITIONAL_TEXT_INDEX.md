# TT-024 Positional Text Index

Status: adopted as an opt-in `hatSchema.MaterializedSource` index for SQL
`CONTAINS_PHRASE` and `CONTAINS_PROXIMITY`.

## API

```go
report, err := source.BuildTextIndex("body")
available := source.HasTextIndex("body")
rows := source.LookupText("body", "alpha beta", 0)
```

`BuildTextIndex` validates the field, snapshots rows, builds a token-to-sorted-
posting map, and publishes it only when the source generation is unchanged.
Each posting stores the row position and every normalized token position in that
row, so repeated tokens remain searchable. Inserts and `Upsert` updates maintain
the index after publication.

The SQL adapter automatically exposes the index for direct `CACHE` sources.
The executor still evaluates the complete phrase or proximity predicate on the
returned candidates, so the index is a narrowing optimization rather than a
second source of truth.

## Defaults And Tradeoff

The index is disabled by default. A source without an explicit
`BuildTextIndex` call uses the existing full scan and has no additional per-row
text-index metadata. Building or maintaining the index costs CPU and memory;
it is appropriate for read-heavy workloads with selective phrase queries.

The index build is generation-checked and retries around concurrent writes. The
benchmark's build `B/op` is cumulative allocation during construction, not the
retained index footprint. Go map, slice-header, allocator, and token-string
overhead are not separately accounted for by `testing.B`.

## Benchmark

Command:

```text
make benchmark-tt024-text-index
```

Linux/amd64, AMD Ryzen 9 5950X, five samples, 20,000 rows, 100 benchmark
iterations per sample, `-benchmem`:

| Path | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Full scan phrase query | 27,020,082 | 23,558,076 | 180,068 |
| Warm positional-index phrase query | 37,468 | 30,896 | 210 |
| Text-index build | 32,756,321 | 26,255,546 | 260,258 |

The warm indexed query is approximately 721x faster, uses 99.9% less timed
allocation volume, and performs 857x fewer allocations. The cost is an
approximately 33 ms build and 26.3 MB of cumulative construction allocation in
this fixture; the index remains opt-in so workloads that cannot justify that
cost keep the old path.

Raw benchmark output is recorded in `BENCHMARK.md`.
