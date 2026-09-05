# SQL Index Advisor Persistence

`SQLIndexAdvisor` records bounded candidate fields from observed slow scans.
The advisor is already opt-in through `QueryOptions.IndexAdvisor`; persistence
lets a process hand recommendations to a later process without retaining query
text, predicate values, or row data.

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

The snapshot is versioned JSON. `Load` accepts at most
`DefaultSQLIndexAdvisorSnapshotMaxBytes` (1 MiB), limits each key and field to
1 KiB, rejects unknown or trailing JSON, duplicate entries, zero counts,
unsupported versions, and snapshots larger than the advisor capacity. It
validates the complete snapshot before replacing live state, so a rejected or
truncated file cannot partially change recommendations.

Persistence is explicit. Existing advisor construction, query execution, and
the default empty advisor state are unchanged. Treat the snapshot as
operational metadata and apply the same filesystem permissions and access
controls used for other service state.
