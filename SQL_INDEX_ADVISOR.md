# SQL Index Advisor Persistence

`SQLIndexAdvisor` records bounded candidate fields and conjunction prefixes from
observed slow scans. The advisor is already opt-in through
`QueryOptions.IndexAdvisor`; persistence lets a process hand recommendations to
a later process without retaining query text, predicate values, or row data.

```go
advisor := hatSql.NewSQLIndexAdvisor(256)

// Use advisor in QueryOptions while executing trusted application queries.
// ...

file, err := os.Create("index-advisor.json")
if err != nil {
	return err
}
defer file.Close()
if err := advisor.Save(file); err != nil {
	return err
}

restored := hatSql.NewSQLIndexAdvisor(256)
file, err = os.Open("index-advisor.json")
if err != nil {
	return err
}
defer file.Close()
if err := restored.Load(file); err != nil {
	return err
}
recommendations := restored.Recommendations()
```

For simple unindexed equality projections, the same opt-in advisor can also
return covering-index candidates:

```go
for _, recommendation := range advisor.CoveringRecommendations() {
	// Review the recommendation before changing the live index configuration.
	if err := trie.CreateSQLJSONCoveringIndex(
		recommendation.Key,
		recommendation.Field,
		recommendation.Columns..., // predicate field is retained automatically
	); err != nil {
		return err
	}
}
```

`CoveringRecommendations` only considers a single `CACHE` source with a
literal equality predicate and direct selected fields. Joins, aggregates,
grouping, ordering, distinct queries, windows, CTEs, unions, computed
expressions, and non-equality predicates are excluded. The columns are sorted,
deduplicated, and exclude the predicate field so the result can be passed
directly to `CreateSQLJSONCoveringIndex` after review. The advisor never creates
or activates an index automatically.

`Save` and `Load` continue to persist the existing predicate-field observations
in the version-2 snapshot format and also persist bounded ordered conjunction
prefixes. Version-1 snapshots remain readable; they simply have no prefix
history. Covering recommendations are derived in-memory from the current
workload and are intentionally regenerated.

For workload-derived primary-key prefix advice, request the most frequently
observed conjunction for each source:

```go
for _, recommendation := range advisor.PrimaryPrefixRecommendations(3) {
	// Review the fields before creating a new ordered layout.
	fmt.Println(recommendation.Key, recommendation.Fields, recommendation.SlowQueries)
}
```

`maxFields` limits the returned prefix length; zero keeps the complete observed
conjunction. Equality predicates are placed before range predicates, and ties
are deterministic. This is read-only advice: it never changes an existing
primary layout or query plan, because automatic layout mutation would require
an explicit rebuild and operational approval.

The snapshot is versioned JSON. `Load` accepts at most
`DefaultSQLIndexAdvisorSnapshotMaxBytes` (1 MiB), limits each key and field to
1 KiB, limits each prefix to 64 fields, rejects unknown or trailing JSON,
duplicate entries, duplicate prefix fields, zero counts, unsupported versions,
and snapshots larger than the advisor capacity. It validates the complete
snapshot before replacing live state, so a rejected or truncated file cannot
partially change recommendations.

Persistence is explicit. Existing advisor construction, query execution, and
the default empty advisor state are unchanged. Treat the snapshot as
operational metadata and apply the same filesystem permissions and access
controls used for other service state.
