# External Lookup Arrangements

`hatSql.LookupSourceResolver` lets an external or remote source answer a
literal equality predicate from its own maintained arrangement. It is
optional; when it is absent or returns `available == false`, SQL uses the
existing full external scan.

```go
type RemotePeople struct {
	rows []hatSql.Row
}

func (source RemotePeople) ResolveSQLLookupSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	if name != "EXTERNAL" || key != "people" || field != "id" {
		return nil, false, nil
	}
	return source.lookupByID(value), true, nil
}
```

For a query such as:

```sql
FROM EXTERNAL('people') AS person
WHERE person.id = 42
SELECT person.name
```

the executor asks the lookup arrangement for candidates, then evaluates the
full SQL predicate against those candidates. This preserves correctness when
an arrangement is stale, approximate, or intentionally returns a superset.
The resolver receives the logical source kind, source key, field, and typed
literal value; it never receives a filesystem path. Existing external source
resolvers and existing `CACHE` index interfaces are unchanged.
