# JSON LIKE Prefix Index

`CreateSQLJSONFieldIndex` already keeps an ordered representation for JSON
field range scans and compatible `ORDER BY` queries. Simple binary-collation
prefix predicates now reuse that representation:

```sql
FROM CACHE('people') AS person
WHERE person.name LIKE 'al%'
SELECT person.id
```

The index is still explicit and opt-in:

```go
if err := trie.CreateSQLJSONFieldIndex("people", "name"); err != nil {
	return err
}
```

The prefix path is used only when all of these are true:

- the source is a `CACHE` JSON source;
- the predicate is a direct field and a literal pattern;
- the collation is binary;
- the pattern has one trailing `%` and no earlier `%`, `_`, or escape byte;
- every present, non-NULL indexed value is a string.

The resolver binary-searches the sorted values, returns candidates, and lets
the normal executor evaluate `LIKE` again. Mixed values deliberately fall
back to a scan because the existing evaluator stringifies non-string values.
Complex patterns, missing indexes, admission denial, and unavailable sources
also retain the existing scan path. Index refresh remains generation-aware, so
replacement of the source cannot return stale candidates.

The feature adds no new per-row structure beyond one boolean on the existing
field-index state and does not change the default when no index is configured.
