# CH-U49: Index EXPLAIN Diagnostics

`EXPLAIN ANALYZE` can now report the work performed by the selected JSON
equality index. This makes it possible to distinguish a useful index from one
that admits most rows without inspecting query results or parsing the
human-readable plan detail.

## Scope

The built-in `hatCache.HatTrie` resolver reports diagnostics for configured
JSON field, typed-int64, bitmap, and JSON-path skip indexes when all of these
conditions hold:

- the source is a `CACHE` source;
- the planner selected one of the supported equality indexes;
- the predicate is a direct equality probe for that indexed path; and
- the query uses `EXPLAIN ANALYZE`.

Composite, covering, lower/text, multikey, compound predicates, unsupported
expressions, and sources without the optional resolver retain their existing
behavior. The feature is diagnostic-only and does not change query results,
index selection, storage, or wire formats.

The `kind` values are `json_field`, `json_typed_int64`, `json_bitmap`, and
`json_path_skip`. Row-oriented indexes report zero for the segment counters;
their candidate and skipped row counts remain exact.

## Configuration And Example

All built-in indexes remain opt-in. The skip-index example uses its existing
bounded configuration:

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

For `json_path_skip`, `index_bytes` is the exact bitmap payload size (`uint64`
words), excluding Go object and map overhead. Other built-in indexes report a
bounded logical footprint estimate for their postings/ordered metadata, also
excluding Go object and map overhead. `candidate_rows` counts rows admitted by
the selected index; the exact SQL predicate still checks those rows.
`skipped_rows` is the remaining source-row count. Segment counters are only
meaningful for the JSON-path skip index.

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

`hat/hatCache/ch_u49_skip_index_explain_test.go` and
`hat/hatCache/ch_u49_index_diagnostics_test.go` verify ordinary query results,
selected-index counters, residual pruning counters, JSON round-trip, and
monitoring/catalog resolver forwarding. Reproduce the focused suite with:

```text
make test-chu49-c203
make race-chu49-c203
make vet-chu49-c203
```

The benchmark and raw samples are recorded in
[BENCHMARK.md](BENCHMARK.md#ch-u49-index-explain-diagnostics).
