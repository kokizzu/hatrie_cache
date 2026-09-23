# M216: Incremental Top-K

## Status

The requested incremental Top-K capability was already implemented as the
`hatSql.IncrementalTopK` maintainer in C213. M216 adds verification and records
the tradeoff instead of introducing a duplicate implementation.

The maintainer keeps an exact weighted relation ordered by the configured SQL
order key. A signed update changes one row's multiplicity, and the maintainer
returns only the changes to the first `K` positions.

```go
topK, err := hatSql.NewIncrementalTopK(hatSql.IncrementalTopKDefinition{
	K:          20,
	OrderKey:   scoreOrderKey,
	Descending: true,
})
changes, err := topK.Apply([]hatSql.DifferentialRow{
	{Key: "row-1", Diff: 1, Row: hatSql.Row{"score": int64(42)}},
})
snapshot := topK.Snapshot()
```

`ApplyWithRankChanges` additionally reports rank-only movement. `Snapshot`
returns at most `K` selected rows, while `AllRows` exposes the complete active
relation for callers that need it.

## Correctness Coverage

`make m216-test` runs the existing C213 tests, including:

- signed weighted inserts, retractions, and replacement after deletion;
- stable key tie-breaking and defensive row cloning;
- atomic validation for missing rows, row conflicts, overflow, and negative
  multiplicity;
- nil receiver and invalid definition handling;
- randomized comparison against a reference implementation.

The focused suite passed:

```text
ok  hatrie_cache/hat/hatSql  0.907s
```

## Benchmark

Command:

```text
make m216-benchmark
```

Workload: five `-benchmem` samples on Linux/amd64 with an AMD Ryzen 9 5950X.
Each iteration updates one row in a 10,000-row relation with `K=20`. The
rebuild baseline sorts all 10,000 rows and derives the changed Top-K rows. The
incremental path updates its ordered treap and emits the selected changes.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full rebuild and sort | 85,996 | 1,089 | 7 | 1.00x | 1.00x |
| Incremental Top-K | 3,158 | 1,213 | 7 | 0.0367x (`27.2x` faster) | 1.114x (`11.4%` higher) |

Raw samples:

```text
BenchmarkMZ037TopKRebuildBaseline: 85996 1089 7; 84782 1089 7; 88429 1089 7; 86790 1090 7; 85688 1090 7
BenchmarkMZ037TopKIncremental:     3065  1213 7; 3293  1212 7; 3298  1213 7; 3158  1212 7; 2770  1212 7
```

The result is a CPU win for update-heavy workloads, but it is not a memory
win in this benchmark: both paths allocate seven objects per measured update,
and the incremental path allocates slightly more bytes because it constructs
the differential result. The benchmark's `B/op` is transient allocation, not
retained heap.

## State-Boundedness Tradeoff

The selected output is bounded by `K`, but exact arbitrary deletes and
replacement promotion require retaining every active candidate, not only the
current winners. `IncrementalTopK` therefore stores all active candidates in
its ordered treap and key map. A truly `O(K)` state would need an external
authoritative source or would have to give up exact replacement behavior.

No approximate bounded-memory mode was added: it would silently lose the
candidate needed when a selected row is deleted. This is the important
limitation when using the current implementation for large relations.
