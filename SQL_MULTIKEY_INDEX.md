# SQL Multikey Array Index

`CreateSQLJSONMultikeyIndex` adds an opt-in membership index for a JSON array
field. It is used for the `ARRAY_CONTAINS` SQL predicate and is disabled unless
the application creates it explicitly.

## Create And Query

```go
if err := trie.CreateSQLJSONMultikeyIndex("people", "tags"); err != nil {
	return err
}

result, err := hatCache.ExecuteSQLQuery(`
FROM CACHE('people') AS person
WHERE ARRAY_CONTAINS(person.tags, 'go')
SELECT person.id`, trie)
```

For this source:

```json
[
  {"id": 1, "tags": ["go", "sql", "go"]},
  {"id": 2, "tags": ["rust"]}
]
```

the result is:

```text
[{"id":1}]
```

Repeated values in one array produce one posting for that row. Missing fields,
empty arrays, `null` elements, and scalar values do not create membership
postings. The executor evaluates `ARRAY_CONTAINS` again after the index returns
candidates, so the index is a filter and never changes predicate semantics.

## Refresh And Diagnostics

The index refreshes lazily from the source generation, using the same source
snapshot and admission controls as the other SQL JSON indexes. A source
replacement is therefore visible after the next indexed query without a
manual rebuild call.

`SQLJSONIndexHealth("people", "tags")` reports source rows, indexed rows,
null/non-indexed rows, distinct typed posting keys, source bytes, and
freshness. `CheckSQLJSONIndexConsistency("people")` reports the index as
`kind: "multikey"` and independently rebuilds a candidate for comparison.

The public resolver method is:

```go
rows, available, err := trie.ResolveSQLMultikeySource(
	"CACHE", "people", "tags", "go",
)
```

`available` is false when the requested multikey index is absent or when the
query shape is not safe for this index. This lets callers and the SQL planner
fall back without treating an unsupported index as an empty result.

## Compatibility Rules

The fast path supports a field reference plus a literal in exactly two
`ARRAY_CONTAINS` arguments under binary collation. Unicode case-insensitive
collation deliberately uses the ordinary scan path because its normalized text
keys are different from the binary index keys.

Mixed JSON types preserve the existing SQL equality rules. For example, a
numeric JSON value `1` compares equal to the string `"1"` under the current SQL
comparison implementation, while `"01"` does not. A mixed source retains a
comparison-key posting map only when needed; homogeneous arrays keep the
compact typed postings. Scalar equality, prefix, range, ordered, borrowed,
and keyset resolver APIs never claim a multikey index.

## Cost And Benchmark

The index retains one posting per distinct array element per source row, so
resident memory is proportional to the number of indexed memberships rather
than only the number of rows. Long arrays and low-selectivity values can make a
full scan preferable for some workloads; the index is intentionally opt-in.

The measured 10,000-row selective fixture produced a median `15,556,089`
ns/op full scan versus `67,498` ns/op with the warmed multikey index: `230.5x`
faster query CPU time, `55.7x` lower transient bytes, and `325.5x` fewer
transient allocations. Those `B/op` figures do not measure resident index
memory. Reproduce the run with:

```sh
make benchmark-sql-multikey
```
