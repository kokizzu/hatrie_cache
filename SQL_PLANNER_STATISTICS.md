# SQL Planner Statistics

`HatTrie` provides an explicit, source-versioned `ANALYZE` operation for JSON
cache sources. It is opt-in: ordinary writes and ordinary SQL queries do not
scan or retain planner statistics.

## Usage

```go
statistics, err := trie.AnalyzeSQLSource("CACHE", "people", "age", "state")
if err != nil {
	return err
}
fmt.Println(statistics.Rows, statistics.Fields["age"].DistinctValues)
```

The result contains:

- source row and encoded-byte counts;
- per-field non-null row counts and null/missing row counts;
- distinct value cardinality;
- numeric minimum and maximum when all observed values are numeric;
- average JSON value size; and
- a frequency histogram that records how many distinct values occur once,
  twice, and so on, without retaining the values themselves.

After analysis, the existing what-if planner consumes the cached result:

```go
report, err := hatCache.ExplainSQLWhatIf(ctx, hatCache.SQLWhatIfRequest{
	Query: `FROM CACHE('people') AS p WHERE p.age = 21 SELECT p.id`,
	Index: hatCache.SQLWhatIfIndex{
		Kind:  hatCache.SQLWhatIfIndexEquality,
		Fields: []string{"age"},
	},
}, trie)
```

The provider is deliberately read-only and never analyzes implicitly. Before
analysis, or after a source mutation, it returns `available=false`, so the
existing planner fallback remains responsible for correctness.

## Consistency And Limits

Statistics are tagged with the cache mutation epoch. A source mutation makes
all cached entries unavailable; the next explicit analysis rebuilds the
requested source. Staged snapshot restore and rollback clear the cache as
derived metadata. The cache holds at most 128 source entries; an analysis
prunes stale entries first and then evicts one entry if the bound is full.

The in-memory statistics cache remains process-local derived state. For
restart or recovery workflows that want to retain analyzed metadata, use the
separate explicit snapshot API:

```go
if err := trie.SaveSQLPlannerStatistics("planner-stats.hps"); err != nil {
	return err
}
```

Restore the cache data first, then load the planner metadata into that cache:

```go
report, err := restored.LoadSQLPlannerStatistics("planner-stats.hps")
if err != nil {
	return err
}
fmt.Println(report.Loaded, report.Skipped)
```

The `HPS1` file is a separate bounded binary artifact with deterministic
ordering and a CRC32 payload checksum. Each entry also stores a SHA-256 digest
of the source bytes. Missing or changed sources are skipped and reported, so a
snapshot from another cache state cannot silently create unsafe estimates. A
load publishes nothing until the complete file and all accepted entries have
been validated. The API is opt-in and does not change normal cache backup
bytes, command wire formats, or startup behavior; include the file in an
operator backup set only when preserving warm planner metadata is useful.

The analysis scan is intentionally explicit because high-cardinality fields
need temporary distinct-value maps and can consume substantial CPU and heap.

## Measured Tradeoff

On the benchmark fixture with 10,000 JSON rows, five-run medians were:

| Operation | Time | Cumulative bytes | Allocations |
| --- | ---: | ---: | ---: |
| Analyze 2 fields | 14.44 ms | 5.92 MB | 190,044 |
| Cached statistics lookup | 536 ns | 1.04 KB | 5 |
| What-if without analysis | 18.18 ms | 8.05 MB | 230,036 |
| What-if with analysis | 3.19 us | 4.03 KB | 22 |

The analyzed what-if path is about 5,701x faster, uses about 1,998x fewer
cumulative bytes, and uses about 10,456x fewer allocations than repeatedly
decoding the source for the same estimate. The one-time analysis is the
tradeoff: it costs about 14 ms and 5.92 MB on this fixture. There is no wire
format change and therefore no bandwidth benefit or cost for ordinary requests.

The persistence-specific benchmark uses the same 10,000-row fixture and five
runs for each path:

```text
make benchmark-before-tt050-durable-stats-c292
make benchmark-tt050-durable-stats-c292
```

The after run kept the ordinary `ANALYZE` path effectively unchanged at
16.48 ms, 5.92 MB, and 190,045 allocations. Saving a snapshot took 1.80 ms,
allocated 383.7 KB transiently, and wrote 117 bytes for the two-field fixture.
Loading it took 0.280 ms, allocated 379.9 KB transiently, and used 24
allocations. The load path is about 59x faster than re-analysis, with 15.6x
lower transient bytes and 7,919x fewer allocations; these are recovery-path
measurements, not a claim that snapshot loading reduces retained cache data.

Run the reproducible benchmark with:

```text
make benchmark-tt050-sql-planner-statistics
```
