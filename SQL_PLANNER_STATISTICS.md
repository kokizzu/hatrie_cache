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

The statistics are process-local derived state. They are not part of data
backups or wire payloads, so restart and restore require re-running analysis.
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
or persistence-format change and therefore no bandwidth benefit or cost.

Run the reproducible benchmark with:

```text
make benchmark-tt050-sql-planner-statistics
```
