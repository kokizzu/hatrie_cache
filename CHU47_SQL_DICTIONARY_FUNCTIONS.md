# CH-U47 SQL Dictionary Functions

Hatrie Cache now has an opt-in named SQL dictionary registry for small,
frequently-read reference data such as region labels, status descriptions, or
feature flags. It is deliberately separate from the existing columnar and
wire dictionaries: those compress values, while this registry provides a
stable SQL lookup contract.

## API

```go
registry := hatSql.NewSQLDictionaryRegistry()
if err := registry.Register("regions", map[string]interface{}{
    "east": "Singapore",
    "west": "Tokyo",
}); err != nil {
    return err
}

// Replaces the complete table and publishes version 2 atomically.
if err := registry.Refresh("regions", map[string]interface{}{
    "east": "Singapore",
    "west": "Tokyo",
    "north": "Seoul",
}); err != nil {
    return err
}
info, err := registry.Info("regions")
// info.Name == "regions", info.Version == 2, info.Entries == 3
```

`Register` starts at version 1 and rejects duplicate names. `Refresh` copies
and validates the complete replacement before publishing it. A failed refresh
leaves the prior version unchanged. Dictionary names are case-insensitive;
keys are canonicalized from strings, bytes, booleans, integers, finite
floating-point values, and `time.Time` values.

The registry itself implements `hatSql.FunctionResolver`. Embed it in the
resolver passed to `ExecuteSQLQuery`:

```go
type resolver struct {
    rows []hatSql.SQLRow
    *hatSql.SQLDictionaryRegistry
}

func (r resolver) ResolveSQLSource(string, string) ([]hatSql.SQLRow, error) {
    return r.rows, nil
}

result, err := hatSql.ExecuteSQLQuery(`
    SELECT code,
        DICT_GET('regions', code, 'unknown') AS region,
        DICT_HAS('regions', code) AS known,
        DICT_VERSION('regions') AS dictionary_version
    FROM CACHE('events')
`, resolver{rows: rows, SQLDictionaryRegistry: registry})
```

## Functions

| Function | Arguments | Result |
| --- | --- | --- |
| `DICT_GET` | `dictionary_name, key[, default]` | Stored value, the default when the key is missing, or `NULL` without a default. |
| `DICT_HAS` | `dictionary_name, key` | Boolean indicating whether the key exists. A `NULL` key is always a miss. |
| `DICT_VERSION` | `dictionary_name` | The `uint64` version used by the call batch. |

An unknown dictionary is an error rather than a miss, which catches spelling
and deployment mistakes. `DICT_GET` preserves a stored `NULL` as a present
value; use `DICT_HAS` to distinguish it from a missing key.

Every vectorized function call batch captures one immutable snapshot per
referenced dictionary. A concurrent refresh therefore produces either the old
or new version for a dictionary in that batch, never a partially refreshed
map. Reads do not lock for every row. Refresh is intentionally copy-on-write:
the temporary and published maps overlap until readers release the old
snapshot.

Values are limited to immutable SQL scalars plus copied `[]byte`; nested maps,
slices, pointers, and arbitrary structs are rejected. `Info` reports only
name/version/entry count and never returns dictionary values. The registry is
not an authorization boundary: only expose it to query resolvers whose callers
are allowed to read the registered values.

## Measurements

Run:

```sh
make benchmark-chu47
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X, using 10,000 lookups and a
256-entry dictionary:

| Workload | Median ns/op | B/op | Allocs/op | Interpretation |
| --- | ---: | ---: | ---: | --- |
| Direct `map[string]interface{}` loop | 104,949 | 0 | 0 | Lower-bound control; it does not build SQL results. |
| Vectorized `DICT_GET` batch | 227,717 | 163,857 | 2 | Includes the 10,000-result slice required by SQL execution. |
| `Refresh` of 10,000 entries | 833,819 | 816,054 | 10,034 | Intentional copy-on-write write cost. |

The common-name snapshot fast path reduced the vectorized lookup median from
about 352,516 ns/op to 227,717 ns/op, or 1.55x faster, without changing
allocation count or semantics. Compared with the raw map lower bound, the SQL
function path remains 2.17x slower because it validates calls, captures a
snapshot, and materializes results. The feature is opt-in, so applications that
do not construct a dictionary registry pay no read-path cost.

The raw samples are also recorded in [BENCHMARK.md](BENCHMARK.md#ch-u47-sql-dictionary-functions).
