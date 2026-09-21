# CH-U49: Skip-Index EXPLAIN Diagnostics

`EXPLAIN ANALYZE` can now report the work performed by the selected JSON-path
skip index. This makes it possible to distinguish a useful skip index from one
that admits most of its segments without inspecting query results or parsing
the human-readable plan detail.

## Scope

The built-in `hatCache.HatTrie` resolver reports diagnostics for a configured
`SQLJSONPathSkipIndexSpec` when all of these conditions hold:

- the source is a `CACHE` source;
- the planner selected the JSON-path skip index;
- the predicate is a direct equality probe for that indexed path; and
- the query uses `EXPLAIN ANALYZE`.

Other index kinds, compound predicates, unsupported expressions, and sources
without the optional resolver retain their existing behavior. The feature is
diagnostic-only and does not change query results, index selection, storage,
or wire formats.

## Configuration And Example

The skip index remains opt-in. Its existing bounded configuration is used:

```go
if err := trie.CreateSQLJSONPathSkipIndex(hatCache.SQLJSONPathSkipIndexSpec{
	CacheKey:       "people",
	Paths:          []string{"$.profile.city"},
	RowsPerSegment: 2,
	BitsPerSegment: 256,
}); err != nil {
	return err
}

query := "FROM CACHE('people') AS p WHERE JSON_VALUE(p.profile, '$.city') = 'Singapore' SELECT p.id"
result, err := hatSql.ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
if err != nil {
	return err
}
for _, step := range result.Plan {
	if step.Index != nil {
		fmt.Println(step.Index.Kind, step.Index.SkippedRows)
	}
}
```

The example's plan step contains an `hatSql.SQLIndexDiagnostics` value like:

```json
{
  "kind": "json_path_skip",
  "field": "$.profile.city",
  "index_bytes": 64,
  "total_rows": 4,
  "candidate_rows": 2,
  "skipped_rows": 2,
  "segments": 2,
  "candidate_segments": 1,
  "skipped_segments": 1
}
```

`index_bytes` is the exact bitmap payload size (`uint64` words), excluding Go
object and map overhead. `candidate_rows` counts all rows in segments admitted
by the Bloom metadata; the exact SQL predicate still checks those rows.
`skipped_rows` and `skipped_segments` are the rows and segments rejected by
the coarse metadata.

The corresponding `ExplainStep.Pruning` reports the exact residual work:
`total_rows=4`, `skipped_rows=2`, `scanned_rows=2`, `matched_rows=1`, and
`residual_rows=1`. The residual rate is the percentage of admitted rows
rejected by the exact predicate. It is a usefulness measure, not a claim that
each residual row came from a distinct Bloom false-positive segment.

The JSON plan contains the typed `index` object. The tabular EXPLAIN result
adds an `index` column only when at least one plan step has index diagnostics;
plans without diagnostics keep their previous column shape.

## Extension Boundary

Applications with a different source index can implement
`hatSql.SQLIndexDiagnosticsResolver` and return a bounded
`hatSql.SQLIndexDiagnostics` value for the selected equality probe. Returning
`available=false` preserves the normal EXPLAIN output. The public payload
contains only index identity, sizes, and row/segment counts; it does not expose
indexed values or source rows.

## Verification

`hat/hatCache/ch_u49_skip_index_explain_test.go` verifies the ordinary query
result, selected-index counters, residual pruning counters, JSON round-trip,
and monitoring/catalog resolver forwarding. Reproduce the focused suite with:

```text
make test-chu49-c203
make race-chu49-c203
make vet-chu49-c203
```

The focused feature-only verification target is `make verify-chu49-focused`.
The broader package targets also exercise unrelated SQL and checkpoint suites;
their current baseline failures are not caused by this diagnostic path.

The benchmark and raw samples are recorded in
[BENCHMARK.md](BENCHMARK.md#ch-u49-skip-index-explain-diagnostics).
