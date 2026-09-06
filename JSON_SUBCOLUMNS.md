# Shared JSON Subcolumns

`hatSql.JSONSubcolumnRegistry` interns repeated JSON paths once per process and
gives callers compact `uint32` IDs for metadata, indexes, or column layouts.

```go
registry := hatSql.NewJSONSubcolumnRegistry()
id, created, err := registry.Intern("$.user.id")
sameID, createdAgain, err := registry.Intern(" $.user.id ")
path, ok := registry.Path(id)
```

Paths are trimmed, empty paths and paths containing NUL are rejected, and
repeated paths return the original ID without creating another metadata entry.
IDs are assigned in first-seen order and are process-local; persist the path
strings or a versioned schema if IDs must survive restart or cross a wire
boundary. `Snapshot` returns independently owned ID-ordered metadata, while
`Lookup` and `Path` are read-only.

The registry uses a read/write lock and is safe for concurrent `Intern`,
`Lookup`, `Path`, and `Snapshot` calls. It shares path metadata but does not
parse JSON, materialize values, or silently create an index. Callers can use
the IDs as keys for their own JSON subcolumn values, posting lists, or
statistics, avoiding repeated path strings in those structures.

Focused coverage is in `hat/hatSql/json_subcolumns_test.go`, including
normalization, stable sharing, lookup and snapshots, invalid paths, concurrent
interning, nil behavior, and an allocation-reporting existing-path benchmark.
