# CH-U47 SQL Dictionary Lookup Functions

CH-U47 adds a bounded, versioned dictionary registry that implements the
existing `hatSql.SQLFunctionResolver` contract. It is an application-owned
lookup layer for reference data such as country codes, feature flags, or
tenant metadata. It does not replace the internal columnar dictionaries used
for storage compression.

## Register A Dictionary

```go
registry, err := hatSql.NewSQLDictionaryRegistry(64)
if err != nil {
	return err
}
err = registry.Register(hatSql.SQLDictionaryDefinition{
	Name:    "countries",
	Version: 1,
	Lookup: func(key interface{}) (interface{}, bool, error) {
		value, found := map[string]string{
			"sg": "Singapore",
			"id": "Indonesia",
		}[key.(string)]
		return value, found, nil
	},
})
```

Names are case-insensitive and stored in normalized lower case. The registry
has a bounded number of definitions. A nonpositive capacity uses the default
of 64; capacities above 4096 are rejected. Dictionary names are limited to
128 UTF-8 bytes. Versions must be positive and fit in a signed SQL integer.

The lookup callback is captured as an immutable definition and is called
outside the registry lock. It should read a caller-owned immutable snapshot or
provide its own synchronization. Returned values are not copied.

## SQL Functions

The registry supports the following vectorized functions:

| Function | Arguments | Result |
| --- | --- | --- |
| `DICT_GET(name, key)` | dictionary name and key | value, or SQL `NULL` on miss |
| `DICT_GET(name, key, default)` | dictionary name, key, and fallback | value, or fallback on miss |
| `DICT_HAS(name, key)` | dictionary name and key | boolean |
| `DICT_VERSION(name)` | dictionary name | signed integer version |

Example:

```sql
SELECT code,
       DICT_GET('countries', code, 'Unknown') AS country,
       DICT_HAS('countries', code) AS known
FROM CACHE('items')
```

The query resolver must implement both `ResolveSQLSource` and
`EvaluateSQLFunction`. An application can forward `EvaluateSQLFunction` to
the registry while retaining its existing source resolver. A missing
dictionary, invalid argument shape/type, or lookup callback error is returned
as an error; a missing key is not an error.

## Refresh And Recovery

```go
err = registry.Refresh(hatSql.SQLDictionaryDefinition{
	Name:    "countries",
	Version: 2,
	Lookup:  refreshedLookup,
})
```

Refresh replaces the callback atomically for future calls and must strictly
increase the dictionary version. A downgrade or refresh of an unknown name is
rejected. `Snapshot` returns deterministic name-sorted definitions for
diagnostics; `Remove` makes subsequent lookups fail explicitly.

The registry itself is in-memory. Persist source data and the last accepted
version in the caller's configuration or control plane, then register the
corresponding immutable callback after restart. This keeps credentials and
dictionary payloads outside SQL query text and the registry's metadata.

## Operational Boundaries

- Use a bounded registry and bound the source snapshot behind each callback.
- Do not put secrets in dictionary names or values returned to untrusted SQL
  users; function authorization remains the caller's responsibility.
- Refresh is monotone per dictionary, but it does not distribute updates to
  other processes.
- A callback can be expensive or perform I/O; the registry does not add a
  timeout. Add timeout/caching in the callback or caller-owned adapter.
- The SQL execution path remains unchanged when the resolver does not expose a
  function resolver.

The focused test covers hits, NULL/default misses, version reads, monotone
refresh, SQL execution, invalid calls, callback errors, capacity, removal, and
concurrent refresh/lookups. `BENCHMARK.md` records the direct map control and
the vectorized registry cost.
