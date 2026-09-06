# Shared JSON Subcolumns

`hat/hatSql.JSONSubcolumnRegistry` interns repeated JSON paths and assigns
process-local IDs. Indexes or column metadata can store the ID instead of
repeating the same path string.

```go
registry := hatSql.NewJSONSubcolumnRegistry()
id, created, err := registry.Intern("$.user.id")
sameID, _, err := registry.Intern(" $.user.id ")
```

Whitespace is trimmed, duplicate paths return the existing ID, and IDs are
assigned in first-seen order. `Lookup` does not create a path, `Path` reverses
an ID, and `Snapshot` returns independent ID-ordered metadata. Empty and NUL
containing paths are rejected. IDs are process-local and must not be treated
as stable wire or disk identifiers without persisting the path snapshot.

The registry is safe for concurrent interning and lookup. Repeated interning of
an already-known path performs no allocation; the registry itself retains one
map key and one string header per unique path.
