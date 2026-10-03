# T-U24 Conditional Space Indexes

hatSchema.MaterializedSource now supports opt-in conditional functional
indexes. A caller supplies both:

- a Predicate string for planner/explain metadata; and
- a Matches(Row) function that decides whether a row is admitted.

The matcher is authoritative. The predicate string is never parsed or treated
as proof that a query may use the index.

## API

~~~
report, err := source.BuildConditionalFunctionalIndex(
    "active_name",
    []string{"name", "active"},
    hatSchema.ConditionalFunctionalIndexOptions{
        Predicate: "active = true",
        Matches: func(row hatSchema.Row) (bool, error) {
            return row["active"] == true, nil
        },
    },
    func(row hatSchema.Row) (interface{}, error) {
        return row["name"], nil
    },
)
~~~

The build snapshots rows, evaluates the matcher and expression outside the
source lock, retries after concurrent inserts, and atomically publishes only a
generation-consistent index. Later inserts use the same matcher. A matcher or
expression error prevents the row from being published at all.

MaterializedSource.DropIndex removes a maintained index without changing
stored rows. ConditionalIndexMetadata returns deterministic cloned metadata,
and SQLResolverAdapter.SQLConditionalIndexMetadata publishes it through the
hatSql.SQLConditionalIndexMetadataResolver contract.

Conditional indexes are deliberately not automatically selected by the SQL
resolver. A planner must prove that the query predicate implies the declared
predicate; otherwise using the partial postings could produce false negatives.
The schema catalog records this lifecycle metadata in
IndexDefinition.Predicate and rejects predicates on non-functional indexes.

## Benchmark

Five-run go test -benchmem samples on Linux/amd64, AMD Ryzen 9 5950X. The
fixture has 50,000 rows, 100 name keys, and a 7:1 non-admitted-to-admitted
distribution for the selected key.

| Workload | Median ns/op | B/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Conditional lookup | 14,452 | 24,859 | 146 | reference |
| Existing lookup then filter | 123,437 | 172,142 | 1,002 | conditional is 8.54x faster, 6.93x lower heap and allocations |
| Conditional rebuild | 16,886,771 | 20,727,946 | 108,082 | reference |
| Existing functional rebuild | 21,277,410 | 22,373,103 | 151,142 | conditional is 1.26x faster, 1.08x lower heap, 1.40x fewer allocations |

The existing functional lookup control in the feature build was within noise
of the detached pre-change baseline (123,437 versus 124,507 ns/op, with
the same 1,002 allocations), so the default path did not regress. Full raw
samples are in T024_BENCHMARK_RAW.txt.
