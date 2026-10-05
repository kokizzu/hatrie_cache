# `SELECT * EXCEPT`

Hatrie SQL supports the ClickHouse-style `SELECT * EXCEPT (...)` projection
shorthand. It copies every field from the source row except the named fields:

```sql
FROM CACHE('users')
SELECT * EXCEPT (password_hash, internal_note)
```

The exclusion list must contain at least one identifier and cannot repeat a
field name. An exclusion that is absent from a particular row is ignored,
which is useful for sparse JSON-like rows. Existing `SELECT *` behavior is
unchanged. The row map retains the source field names; the shorthand does not
rename or transform values.

The general materialized and streamed executors both apply the exclusion. A
simple untyped, single-source `CACHE` or `VALUES` query with no relational
operators uses a direct projection path that avoids execution-envelope and
source-cache allocations. Queries with filters, joins, grouping, ordering,
typed fields, or other operators retain the existing executor and its
validation semantics.

## Examples

```sql
FROM VALUES (1, 'alice', true) AS users(id, name, active)
SELECT * EXCEPT (name)
```

```text
columns: [column1]
rows:
  {"id": 1, "active": true}
```

The `columns` value above follows the existing wildcard result contract. The
returned row map is the authoritative field-name projection, as it is for the
pre-existing `SELECT *` path.

## Verification

```sh
make test-chu53-select-star-except
make benchmark-chu53-select-star-except
```

The focused tests cover ordinary wildcard compatibility, cache-backed direct
projection, streamed output, `VALUES` fallback execution, and malformed or
duplicate exclusion lists.
