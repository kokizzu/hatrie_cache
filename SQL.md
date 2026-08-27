# SQL CLI support

This document defines the SQL-like client language. It is a client-side
compiler: `hatrie-cli sql` translates command statements to the existing
authenticated `/api/commands` request format. Read-only relational `SELECT`
queries are validated locally and executed through the authenticated
`/api/sql` endpoint against a cache snapshot.

Executable production coverage is tracked in
[SQL_TEST_MATRIX.md](SQL_TEST_MATRIX.md).

## Goals

- [x] Provide familiar SQL forms for scalar cache reads and writes.
- [x] Provide one lossless SQL `CALL` form for every public cache command.
- [x] Compile multiple SQL statements into the existing ordered, non-atomic
      `BATCH` command.
- [x] Report syntax and SQL-to-command validation errors with a Rust-style
      source span and a safe suggestion.
- [x] Keep internal replication commands inaccessible through SQL.
- [x] Expose the feature through `hatrie-cli sql` with unit tests.

## First SQL session

This SQL interface has two different modes. They look similar but solve
different problems:

| Mode | What it reads/writes | Statements |
| --- | --- | --- |
| **Command SQL** | One cache key at a time. It can create, replace, conditionally merge, update, expire, and delete cache values. | `SELECT value|exists|ttl|dump FROM cache`, `INSERT`, `MERGE`, `UPDATE`, `DELETE`, `CALL` |
| **Relational SQL** | A read-only snapshot of JSON rows or cache metadata. It never changes a cache value. | `SELECT ... FROM CACHE(...)`, `KEYS`, `VALUES`, CTEs, joins, grouping, and related clauses |

Do not confuse `SELECT value FROM cache WHERE key = 'name'` with `SELECT ...
FROM CACHE('users')`: the first is a command-SQL lookup of one cache key; the
second is a relational query over JSON rows held in a cache value.

### User-defined table functions

Relational SQL can also read application-provided rows through
`TABLE(function_name(args...))`. The resolver passed to `ExecuteSQLQuery` must
implement `hatSql.TableFunctionResolver`; its `ResolveSQLTableFunction` method
receives the function name and literal or `$1`-style prepared arguments, then
returns `[]hatSql.SQLRow`. The returned rows continue through the ordinary
relational engine, so filters, joins, grouping, ordering, limits, cancellation,
and query budgets retain their normal behavior.

```go
type resolver struct{}

func (resolver) ResolveSQLSource(kind, key string) ([]hatSql.SQLRow, error) {
	return nil, nil
}

func (resolver) ResolveSQLTableFunction(name string, arguments []interface{}) ([]hatSql.SQLRow, error) {
	return []hatSql.SQLRow{{"value": int64(10)}, {"value": int64(11)}}, nil
}

result, err := hatSql.ExecuteSQLQuery(
	"FROM TABLE(series(2, 10)) AS item SELECT item.value ORDER BY item.value",
	resolver{},
)
```

Table functions are deliberately separate from scalar `FunctionResolver`: a
scalar function produces one value for each input row, while a table function
creates a source of rows. `TABLE(...)` function arguments are constants or
prepared parameters rather than row expressions, so a function cannot create
an accidental nested scan for every input row.

### JSON paths and nested indexes

`JSON_VALUE(json, path)` returns a scalar, `JSON_QUERY(json, path)` returns an
object or array, and `JSON_EXISTS(json, path)` reports whether a path exists.
Paths start with `$` and support members and non-negative array indexes, such
as `$.profile.city`, `$.tags[0]`, and `$['display-name']`.

```go
trie.CreateSQLJSONPathIndex("people", "$.profile.city")
result, err := hatSql.ExecuteSQLQuery(
	"FROM CACHE('people') AS p WHERE JSON_VALUE(p.profile, '$.city') = 'Singapore' SELECT p.id",
	trie,
)
```

The path index is lazy and refreshed when the cache value changes. Equality and
range predicates on `JSON_VALUE(column, relative_path)` use it when the created
absolute index path matches the column plus relative path. The full predicate
is still evaluated after the probe, preserving SQL semantics.

### Command SQL: create, read, change, expire, and delete

The following session is intentionally small and sequential. Run each statement
separately to see its reply. When several statements are sent in one SQL
program, the CLI compiles them into a non-transactional `BATCH` and returns one
response per statement in source order.

**Before cache state:** `name=∅`, `views=∅`, and `temporary=∅`. `∅` means that
the key does not exist.

```sql
SELECT exists FROM cache WHERE key = 'name';
INSERT INTO cache (key, value) VALUES ('name', 'Ivi');
SELECT value FROM cache WHERE key = 'name';
UPDATE cache SET value = 'Ada' WHERE key = 'name';
CALL GET('name');

INSERT INTO cache (key, counter) VALUES ('views', 41);
UPDATE cache SET value = value + 1 WHERE key = 'views';

INSERT INTO cache (key, value, ttl_seconds) VALUES ('temporary', 'yes', 60);
SELECT ttl FROM cache WHERE key = 'temporary';
CALL PERSIST(key => 'temporary');
SELECT ttl FROM cache WHERE key = 'temporary';

DELETE FROM cache WHERE key = 'name';
SELECT exists FROM cache WHERE key = 'name';
```

| Statement | Result | State after it runs |
| --- | --- | --- |
| first `SELECT exists` | `0` | `name=∅` |
| `INSERT ... ('name','Ivi')` | `stored string` | `name="Ivi"` |
| `SELECT value` | `Ivi` | unchanged |
| `UPDATE ... value='Ada'` | `stored string` | `name="Ada"` |
| `CALL GET('name')` | `Ada` | unchanged |
| `INSERT ... counter=41` | `stored counter` | `views=41` |
| `UPDATE ... value=value+1` | `42` | `views=42` |
| timed `INSERT` | `stored string with ttl` | `temporary="yes"` with a 60-second expiry |
| first `SELECT ttl` | a positive number of seconds | unchanged |
| `CALL PERSIST` | `ttl removed` | `temporary="yes"` with no expiry |
| second `SELECT ttl` | `-1` | unchanged; `-1` means no expiry |
| `DELETE` then `SELECT exists` | `deleted`, then `0` | `name=∅` |

**After cache state:** `name=∅`, `views=42`, and `temporary="yes"` with no
TTL. `INSERT` is a cache upsert, not a relational-table insert: inserting the
same key again replaces its value. `UPDATE` also targets exactly one cache key
because its `WHERE` clause must be `key = ...`.

### Conditional `MERGE` and `RETURNING`

`MERGE` provides an atomic, key-based conditional upsert. `WHEN NOT MATCHED`
only writes a missing key, `WHEN MATCHED` only writes an existing key, and
including both clauses performs an unconditional upsert. Conditional forms
accept one scalar string or counter value; expiration fields are deliberately
rejected because the condition and expiration must remain one native write.

```sql
MERGE INTO cache (key, value) VALUES ('profile:1', 'Ada')
WHEN NOT MATCHED THEN INSERT
RETURNING key, value, exists;

MERGE INTO cache (key, value) VALUES ('profile:1', 'Grace')
WHEN MATCHED THEN UPDATE
RETURNING key, value;
```

`RETURNING` is available through `ExecuteSQLMutation` on a single direct
`INSERT`, `MERGE`, `UPDATE`, or `DELETE`. It accepts `key`, `value`, `exists`,
`ttl_seconds`, or `*`; an unmet conditional merge returns zero affected rows
and no returned row. `DELETE ... RETURNING` captures its row before deletion.
`INSERT ... SELECT RETURNING` is not supported because it may stage up to the
atomic-batch limit of rows.

### `INSERT ... SELECT`

`ExecuteSQLMutation` also supports selecting many cache writes from a
read-only relational snapshot. The target columns may be `key` with exactly
one of `value` or `counter`, plus optional `ttl_seconds` or `unix_seconds`.
Selected columns map to those target columns by position. It validates every
selected row and applies one atomic cache-command batch, so an invalid key or
value prevents all writes. The atomic form is intentionally limited to the
public batch maximum of 4096 rows.

```go
result, err := hatriecache.ExecuteSQLMutation(ctx, trie, `
INSERT INTO cache (key, value)
FROM VALUES ('user:1', 'Ada'), ('user:2', 'Lin') AS rows(key, value)
SELECT key, value`, nil, hatriecache.SQLQueryOptions{})
// result.Affected == 2
```

The relational query is evaluated before mutation and uses its normal query
context, cancellation, and resource budgets. It is an execution API rather
than `CompileSQL` because a static command request cannot contain rows that
only exist after the relational query runs.

### `CALL`: SQL syntax for every cache command

`CALL` exposes every public cache command without losing any command fields:

```sql
CALL NAME(field => value, other_field => value);
```

Use `=>` for named fields. Values such as a list or object must be explicit
JSON literals, so SQL strings and JSON do not get confused.

**Before cache state:** `user:1=∅`, `tags=∅`, `jobs=∅`.

```sql
CALL MAP.PUT(key => 'user:1', pairs => JSON '{"name":"Ivi","role":"admin"}');
CALL MAP.PEEK(key => 'user:1', subkey => 'name');
CALL SET.ADD(key => 'tags', values => JSON '["go","cache"]');
CALL SET.HAS(key => 'tags', value => 'go');
CALL SLICE.PUSH(key => 'jobs', values => JSON '["build","verify"]');
CALL SLICE.POP(key => 'jobs');
```

| Statement | Result | State after it runs |
| --- | --- | --- |
| `MAP.PUT` | `stored map fields` | `user:1={name:Ivi,role:admin}` |
| `MAP.PEEK` | `Ivi` | map unchanged |
| `SET.ADD` | `2` newly added members | `tags={go,cache}` |
| `SET.HAS` | `1` | set unchanged |
| `SLICE.PUSH` | `pushed slice values` | `jobs=[build,verify]` |
| `SLICE.POP` | `verify` | `jobs=[build]` |

The command inventory below lists every accepted `CALL` spelling, including
aliases and dotted names. Its data-structure behavior is exactly the same as a
direct command request. For the complete before/request/reply/after flow of
every cache command, including filters, sketches, bitmaps, radix trees, and
Fenwick trees, read [DATA_STRUCTURE.md](DATA_STRUCTURE.md). `CALL` changes only
the input syntax; it does not change the stored value or reply semantics.

### Command-SQL errors and safety

- A command for the wrong type, such as `CALL SET.ADD` on a string key, returns
  an error and leaves the old value unchanged.
- Counter arithmetic is signed 32-bit and rejects overflow.
- `ttl_seconds` must be positive. `PERSIST` removes an existing TTL; it does
  not delete the key.
- A multi-statement program is ordered but not atomic: an error in a later
  statement does not undo an earlier successful statement. Use `BEGIN ATOMIC;
  ...; COMMIT;` when every statement maps to a supported scalar command. The
  compiler rejects unsafe forms such as `INC` before any write occurs, rather
  than pretending that a partial rollback is safe.
- Internal replication commands are rejected by the SQL compiler.

## Relational query walkthrough

Relational SQL is read-only. It is for asking questions about JSON records that
your application has already stored as raw bytes. Command SQL does not convert
`INSERT ... value='...'` into a JSON row source. For an in-process application,
the setup looks like this:

```go
trie.UpsertBytes("users", []byte(`[
  {"id":1,"name":"Ada","team_id":10,"enabled":true,"score":9},
  {"id":2,"name":"Ivi","team_id":10,"enabled":true,"score":7},
  {"id":3,"name":"Noa","team_id":20,"enabled":false,"score":7}
]`))
trie.UpsertBytes("teams", []byte(`[
  {"id":10,"name":"Core"}, {"id":20,"name":"Edge"}
]`))
```

**Before query state:** the `users` and `teams` JSON bytes above exist. Every
query below observes one consistent snapshot and leaves both keys unchanged.

### Filter and project rows

```sql
FROM CACHE('users') AS u
WHERE u.enabled = TRUE AND u.score >= 7
SELECT u.name, u.score
ORDER BY u.score DESC, u.name ASC;
```

Result:

| name | score |
| --- | ---: |
| Ada | 9 |
| Ivi | 7 |

`FROM` chooses rows, `WHERE` keeps only rows whose condition is true, `SELECT`
chooses output columns, and `ORDER BY` sorts the result. The source JSON and
cache state do not change.

### Join related JSON values

```sql
FROM CACHE('users') AS u
LEFT JOIN CACHE('teams') AS t ON u.team_id = t.id
WHERE u.enabled = TRUE
SELECT u.name, t.name AS team
ORDER BY u.name;
```

Result:

| name | team |
| --- | --- |
| Ada | Core |
| Ivi | Core |

`LEFT JOIN` keeps every qualifying user even when no team matches; fields from
the missing team become `NULL`. `INNER JOIN` keeps only matching pairs. `RIGHT
JOIN`, `FULL OUTER JOIN`, and `CROSS JOIN` are also supported; see the query
grammar section for their rules.

### Group and aggregate rows

```sql
FROM CACHE('users') AS u
SELECT u.team_id, COUNT(*) AS members, SUM(u.score) AS total_score
GROUP BY u.team_id
HAVING COUNT(*) >= 1
ORDER BY u.team_id;
```

Result:

| team_id | members | total_score |
| ---: | ---: | ---: |
| 10 | 2 | 16 |
| 20 | 1 | 7 |

`GROUP BY` makes one output row per equal `team_id`. `COUNT`, `SUM`, `AVG`,
`MIN`, and `MAX` summarize each group. `HAVING` filters those finished groups;
use `WHERE` to filter input rows before grouping.

### Inline rows, CTEs, distinct values, windows, and pagination

`VALUES` supplies small rows directly in a query; `WITH` gives them a temporary
name. This needs no cache key and is useful for learning, tests, and parameters.

```sql
WITH scores(name, score) AS (VALUES ('Ada', 9), ('Ivi', 7), ('Noa', 7))
SELECT DISTINCT name, score, RANK() OVER (ORDER BY score DESC) AS place
FROM scores
ORDER BY score DESC, name ASC
LIMIT 2;
```

Result:

| name | score | place |
| --- | ---: | ---: |
| Ada | 9 | 1 |
| Ivi | 7 | 2 |

`DISTINCT` removes duplicate projected rows. `LIMIT` keeps the first N rows
after ordering; `OFFSET` skips rows before applying the limit. Window functions
also include `ROW_NUMBER`, `DENSE_RANK`, `LAG`, `LEAD`, and numeric
`SUM`/`AVG`/`MIN`/`MAX` frames.

Use a `ROWS BETWEEN … AND …` frame for moving aggregates. Bounds support
`UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`, `n FOLLOWING`, and
`UNBOUNDED FOLLOWING`; a frame start may not follow its end.

```sql
FROM VALUES (1, 10), (2, 20), (3, 30) AS readings(sample, value)
SELECT sample,
       AVG(value) OVER (ORDER BY sample ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS moving_average
ORDER BY sample;
```

`RANGE BETWEEN ...` uses the distance of one numeric `ORDER BY` value instead
of physical row positions. It includes peers with the same order value and
supports ascending or descending order; multiple order expressions and
non-numeric range values are rejected rather than producing ambiguous results.

```sql
FROM VALUES (1, 10), (2, 20), (4, 40) AS readings(sample, value)
SELECT sample,
       SUM(value) OVER (ORDER BY sample RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS nearby_sum
ORDER BY sample;
```

### Combine query results

```sql
SELECT value FROM VALUES (1), (2) AS left_rows(value)
UNION
SELECT value FROM VALUES (2), (3) AS right_rows(value)
ORDER BY value;
```

`UNION` keeps the first occurrence of each projected row. The executor
deduplicates the left input while building membership, then admits only unseen
right rows, so it does not first materialize a combined duplicate result.
`UNION ALL` preserves every row and bypasses this membership work. Large
distinct set operations retain the existing bounded external-set spill path.

Result: `1`, `2`, `3`. `UNION` removes duplicates; `UNION ALL` preserves them.
`INTERSECT` keeps rows common to both sides, and `EXCEPT` keeps rows present in
the first side but not the second. These operations are read-only.

### Other relational tools

| Need | Use | Result/effect |
| --- | --- | --- |
| Inspect cache metadata | `FROM KEYS SELECT key, type, ttl_ms ORDER BY key` | One read-only row per cache key. |
| Reuse a query | `WITH active AS (...) SELECT ... FROM active` | Named rows exist only for this query. |
| Walk a hierarchy/sequence | `WITH RECURSIVE walk(...) AS (seed UNION ALL step) SELECT ...` | Repeats the recursive term within row/depth budgets; never mutates cache data. |
| Query a query | `FROM (SELECT ... ) AS derived SELECT ...` | Derived table is read-only and uncorrelated. |
| Match text | `WHERE name LIKE 'Ad%'` | Keeps matching non-NULL strings; comparisons are case-sensitive UTF-8 binary. |
| Match one of several values | `WHERE state IN ('open', 'paused')` | `NOT IN` is also supported; a list containing `NULL` follows SQL three-valued logic. |
| Match an inclusive range | `WHERE score BETWEEN 60 AND 100` | `NOT BETWEEN` is supported; either null operand yields unknown. |
| Compare time correctly | `WHERE occurred_at >= TIMESTAMP '2026-08-22T09:00:00Z'` or `day > DATE '2026-08-22'` | Chronological typed comparison; malformed literals are rejected locally. |
| Normalize a dynamic field explicitly | `CAST(raw_score AS NUMBER)` | Supports `TEXT`, `NUMBER`, `DECIMAL`, `BOOLEAN`, `DATE`, and `TIMESTAMP`; invalid dynamic values produce a source-spanned error instead of silently becoming NULL. |
| Validate a JSON source schema | `FROM CACHE('users') AS u(id INTEGER, joined_on DATE)` | Validates and converts declared non-null fields before relational evaluation; a bad row identifies its cache key, row, field, expected type, and source span. |
| Inspect a plan | `EXPLAIN FROM VALUES (1) AS rows(value) SELECT value` | Returns a plan without reading cache sources; simple equality joins are marked as hash-join eligible. |
| Measure one plan | `EXPLAIN ANALYZE FROM ... SELECT ...` | Executes once and returns plan steps plus elapsed/output statistics. |
| Avoid text interpolation | `... WHERE u.id = $1` with separate parameters | Parameters keep their JSON/Go type and are not concatenated into SQL. |
| Page results | `POST /api/sql` with `page_size` and returned `cursor` | Reads the next page from the same query/parameter payload; no mutation. |
| Limit with standard syntax | `FETCH FIRST 20 ROWS ONLY` | Equivalent to `LIMIT 20`; cannot be combined with `LIMIT`. |
| Stream rows | `POST /api/sql` with `"stream":true` | Emits NDJSON column, row, and terminal records without materializing all result rows; finite source-only top-N and configured unbounded scalar external sorting are bounded. |

Streaming also supports a chain of equality `INNER` or `LEFT JOIN`s when the
left `CACHE` source is streamable and every right `CACHE` join field has an
available JSON index. The executor streams the left rows and probes each next
index per current row; it never materializes a join-result slice. Other join
forms and joins without those indexes remain rejected with an explanatory error.

The exact command and relational examples in this section are executed by
`TestSQLGuideCommandExamples` and `TestSQLGuideRelationalExamples`.

### Explicit casts

Use `CAST(expression AS type)` whenever a JSON field's stored representation
needs to change for a comparison, grouping key, or calculation. Supported
targets are `TEXT`, `NUMBER`, `DECIMAL`, `BOOLEAN`, `DATE`, and `TIMESTAMP`. `NULL`
remains `NULL`. Text-to-number/date/timestamp conversions are strict; boolean
conversion accepts `true`/`false` text (case-insensitive) and numeric `0`/`1`.

`DECIMAL '123.45'` and `CAST(raw AS DECIMAL)` preserve arbitrary-precision
decimal comparison semantics rather than reducing the value to `float64`.
Decimal literals accept an optional sign and fractional part, but not exponent
notation or fractions; use a quoted decimal string such as `DECIMAL '10.01'`.

```sql
FROM CACHE('imported-scores') AS scores
WHERE CAST(scores.raw_score AS NUMBER) >= 80
SELECT scores.name, CAST(scores.imported_on AS DATE) AS imported_on
ORDER BY CAST(scores.raw_score AS NUMBER) DESC;
```

If one row contains an incompatible dynamic value, execution stops with the
existing Rust-style source excerpt pointing at `CAST`, rather than treating the
value as a match, an ordering key, or an implicit null.

### Typed JSON source fields

`CACHE` sources may declare the expected type of selected JSON fields directly
after their alias. Supported field types are `TEXT`, `NUMBER`, `INTEGER`,
`DECIMAL`, `BOOLEAN`, `DATE`, `TIMESTAMP`, and `JSON` (object or array).
Declared non-null values are validated once for the query and converted to the
matching SQL type; absent and JSON-null fields remain SQL `NULL`.

```sql
FROM CACHE('users') AS users(
  id INTEGER,
  joined_on DATE,
  balance DECIMAL,
  profile JSON
)
WHERE users.joined_on >= DATE '2026-01-01'
SELECT users.id, users.balance;
```

Schema validation operates on a query-local clone, never mutating the cached
JSON source. An incompatible row stops the query with a Rust-style diagnostic
at the declared type, for example `CACHE("users") row 2 field "id" expects
INTEGER, got TEXT`.

## Command inventory

The SQL language accepts the following public command names in `CALL`.
Aliases accepted by the server are shown after `/`; the first name is the
canonical spelling emitted by documentation and examples.

| Group | Commands |
| --- | --- |
| Pipeline and scalar values | `BATCH`; `GET`/`GETSTR`; `EXISTS`; `SET`/`SETSTR`; `SETX`/`SETSTRX`; `SETINT`; `SETINTX`; `INC`; `DEL`; `TTL`; `EXPIRE`; `EXPIREAT`; `PERSIST`; `DUMP` |
| Maps | `PUTMAP`; `PEEKMAP`; `TAKEMAP` |
| Slices | `PUSHSLICE`; `POPSLICE`; `SHIFTSLICE`; `HEADSLICE`; `TAILSLICE` |
| Sets | `ADDSET`; `REMSET`; `HASSET`; `GETSET` |
| Priority queues | `PUSHPQ`/`PUSHPRIORITY`; `PEEKPQ`/`PEEKPRIORITY`; `POPPQ`/`POPPRIORITY`; `GETPQ`/`GETPRIORITY` |
| Bloom filters | `CREATEBF`/`RESERVEBF`/`BFRESERVE`; `ADDBF`/`BFADD`; `HASBF`/`BFHAS`/`BFEXISTS`; `INFOBF`/`BFINFO` |
| Cuckoo filters | `CREATECF`/`RESERVECF`/`CFRESERVE`; `ADDCF`/`CFADD`; `HASCF`/`CFHAS`/`CFEXISTS`; `DELCF`/`REMCF`/`CFDEL`; `INFOCF`/`CFINFO` |
| XOR filters | `CREATEXF`/`RESERVEXF`/`XFRESERVE`/`CREATEXOR`; `ADDXF`/`XFADD`; `BUILDXF`/`XFBUILD`; `HASXF`/`XFHAS`/`XFEXISTS`; `INFOXF`/`XFINFO` |
| Roaring bitmaps | `CREATERB`/`CREATEROARING`/`RBRESERVE`; `ADDRB`/`RBADD`; `REMRB`/`DELRB`/`RBREM`/`RBDEL`; `HASRB`/`RBHAS`/`RBEXISTS`; `COUNTRB`/`RBCOUNT`; `GETRB`/`RBGET`; `INFORB`/`RBINFO` |
| Sparse bitsets | `CREATESB`/`CREATESPARSEBITSET`/`SBRESERVE`; `ADDSB`/`SBADD`; `REMSB`/`DELSB`/`SBREM`/`SBDEL`; `HASSB`/`SBHAS`/`SBEXISTS`; `COUNTSB`/`SBCOUNT`; `GETSB`/`SBGET`; `INFOSB`/`SBINFO` |
| Radix trees | `CREATERT`/`CREATERADIX`/`RTCREATE`; `PUTRT`/`RTPUT`; `GETRT`/`RTGET`; `DELRT`/`REMRT`/`RTDEL`/`RTREM`; `HASRT`/`RTEXISTS`/`RTHAS`; `PREFIXRT`/`SCANRT`/`RTPREFIX`/`RTSCAN`; `INFORT`/`RTINFO` |
| Count-Min sketches | `CREATECMS`/`RESERVECMS`/`CMSRESERVE`; `INCRCMS`/`ADDCMS`/`CMSADD`; `ESTCMS`/`QUERYCMS`/`CMSQUERY`/`CMSCOUNT`; `INFOCMS`/`CMSINFO` |
| HyperLogLogs | `CREATEHLL`/`RESERVEHLL`/`HLLRESERVE`; `ADDHLL`/`HLLADD`; `COUNTHLL`/`ESTHLL`/`HLLCOUNT`/`HLLCARD`; `INFOHLL`/`HLLINFO` |
| Top-K | `CREATETOPK`/`RESERVETOPK`/`TOPKRESERVE`; `ADDTOPK`/`TOPKADD`; `ESTTOPK`/`QUERYTOPK`/`TOPKCOUNT`; `GETTOPK`/`TOPK`; `INFOTOPK`/`TOPKINFO` |
| Reservoir samples | `CREATERS`/`CREATESAMPLE`/`RESERVERS`/`RSRESERVE`; `ADDRS`/`RSADD`; `GETRS`/`RSGET`/`SAMPLE`; `INFORS`/`RSINFO` |
| Quantile sketches | `CREATEQ`/`CREATEQS`/`CREATEQUANTILE`/`RESERVEQ`/`QSRESERVE`; `ADDQ`/`ADDQS`/`QADD`/`QSADD`; `ESTQ`/`QUERYQ`/`QQUERY`/`QSQUERY`/`QUANTILE`; `INFOQ`/`QINFO`/`INFOQS`/`QSINFO` |
| Fenwick trees | `CREATEFW`/`CREATEFENWICK`/`RESERVEFW`/`FWRESERVE`; `ADDFW`/`FWADD`; `GETFW`/`FWGET`; `SUMFW`/`PREFIXFW`/`FWPREFIX`/`FWSUM`; `RANGEFW`/`FWRANGE`; `INFOFW`/`FWINFO` |

### Preferred dotted aliases and parameters

The established flat names above remain fully supported. The dotted aliases
below are preferred in new SQL and direct command requests because the data
structure and operation are immediately visible. Every operation requires
`key` unless stated otherwise. `value` accepts one scalar; `values` accepts a
JSON array of scalars; `pairs` accepts a JSON object.

> **One logical key namespace:** Hatrie uses separate backing pools internally,
> but each cache key has exactly one value type. `CMS.CREATE(key => 'stats')`
> and `TOPK.CREATE(key => 'stats')` do **not** create isolated values; a
> different-type write can replace the earlier value. Prefix logical keys by
> type, for example `cms:stats` and `topk:stats`.

| Type | Preferred command | Parameters | Effect |
| --- | --- | --- | --- |
| Map | `MAP.PUT` | `key`, `pairs` or `subkey` + `value` | Upsert one or more map fields. |
|  | `MAP.PEEK`, `MAP.TAKE` | `key`, `subkey` | Read, or read-and-remove, a map field. |
| Slice | `SLICE.PUSH` | `key`, `value` or `values` | Append values. |
|  | `SLICE.POP`, `SLICE.SHIFT`, `SLICE.HEAD`, `SLICE.TAIL` | `key` | Remove last, remove first, read first, or read last value. |
| Set | `SET.ADD`, `SET.REM` | `key`, `value` or `values` | Add or remove members. |
|  | `SET.HAS` | `key`, `value` | Test membership. |
|  | `SET.GET` | `key` | Return all members. |
| Priority queue | `PQ.PUSH` | `key`, `priority`, `value` or `values` | Push values at a priority. |
|  | `PQ.PEEK`, `PQ.POP`, `PQ.GET` | `key` | Read best, remove best, or return all entries. |
| Bloom filter | `BF.CREATE` | `key`, `value` = expected items; optional `subkey` = false-positive rate | Create or replace a Bloom filter. |
|  | `BF.ADD`, `BF.HAS`, `BF.INFO` | `key`, `value`/`values`; `key`, `value`; or `key` | Add, test, or inspect. |
| Cuckoo filter | `CF.CREATE` | `key`, `value` = capacity; optional `subkey` = false-positive rate | Create or replace a Cuckoo filter. |
|  | `CF.ADD`, `CF.DEL`, `CF.HAS`, `CF.INFO` | `key`, `value`/`values`; `key`, `value`/`values`; `key`, `value`; or `key` | Add, delete, test, or inspect. |
| XOR filter | `XF.CREATE` | `key`; optional `value` = expected items | Create the staged XOR filter. |
|  | `XF.ADD`, `XF.BUILD`, `XF.HAS`, `XF.INFO` | `key`, `value`/`values`; `key`; `key`, `value`; or `key` | Stage values, build, test, or inspect. |
| Roaring bitmap | `RB.CREATE` | `key` | Create a bitmap. |
|  | `RB.ADD`, `RB.REM`, `RB.HAS`, `RB.COUNT`, `RB.GET`, `RB.INFO` | `key`, uint32 `value`/`values`; same; `key`, uint32 `value`; or `key` | Mutate, test, count, enumerate, or inspect. |
| Sparse bitset | `SB.CREATE` | `key` | Create a sparse bitset. |
|  | `SB.ADD`, `SB.REM`, `SB.HAS`, `SB.COUNT`, `SB.GET`, `SB.INFO` | `key`, uint64 `value`/`values`; same; `key`, uint64 `value`; or `key` | Mutate, test, count, enumerate, or inspect. |
| Radix tree | `RT.CREATE` | `key` | Create a radix tree. |
|  | `RT.PUT` | `key`, `subkey`, `value` | Upsert a subkey. |
|  | `RT.GET`, `RT.DEL`, `RT.HAS` | `key`, `subkey` | Read, delete, or test a subkey. |
|  | `RT.PREFIX`, `RT.INFO` | `key`, optional `subkey` prefix; or `key` | Scan a prefix or inspect the tree. |
| Count-Min sketch | `CMS.CREATE` | `key`, `value` = width; optional `subkey` = depth | Create a sketch. |
|  | `CMS.ADD` | `key`, `value`/`values`; optional `priority` = increment count | Increment estimates for one or more values. |
|  | `CMS.EST`, `CMS.INFO` | `key`, `value`; or `key` | Estimate a value or inspect dimensions. |
| HyperLogLog | `HLL.CREATE` | `key`, optional `value` = precision | Create a cardinality estimator. |
|  | `HLL.ADD`, `HLL.COUNT`, `HLL.INFO` | `key`, `value`/`values`; or `key` | Add items, estimate cardinality, or inspect. |
| Top-K | `TOPK.CREATE` | `key`, optional `value` = capacity | Create a heavy-hitter tracker. |
|  | `TOPK.ADD` | `key`, `value`/`values`; optional `priority` = count | Add observations. |
|  | `TOPK.EST`, `TOPK.GET`, `TOPK.INFO` | `key`, `value`; or `key` | Estimate a value, return leaders, or inspect. |
| Reservoir sample | `RS.CREATE` | `key`, optional `value` = capacity | Create a uniform reservoir. |
|  | `RS.ADD`, `RS.GET`, `RS.INFO` | `key`, `value`/`values`; or `key` | Add observations, return sample, or inspect. |
| Quantile sketch | `Q.CREATE` | `key`, optional `value` = epsilon | Create an approximate quantile sketch. |
|  | `Q.ADD` | `key`, numeric `value`/`values` | Add observations. |
|  | `Q.EST`, `Q.INFO` | `key`, `value` = quantile in [0,1]; or `key` | Estimate a quantile or inspect. |
| Fenwick tree | `FW.CREATE` | `key`, optional `value` = size | Create a prefix-sum tree. |
|  | `FW.ADD` | `key`, `value` = index, `subkey` = signed delta | Add a delta at an index. |
|  | `FW.GET`, `FW.SUM` | `key`, `value` = index | Read a cell or prefix sum. |
|  | `FW.RANGE`, `FW.INFO` | `key`, `value` = start, `subkey` = end; or `key` | Sum an inclusive range or inspect. |

For example:

```sql
CALL CMS.CREATE(key => 'frequency:paths', value => 2048, subkey => 4);
CALL CMS.ADD(key => 'frequency:paths', value => '/home');
CALL TOPK.CREATE(key => 'popular:paths', value => 100);
```

`INTERNALSET`, `INTERNALSETV2`, `INTERNALSETV3`, `INTERNALDEL`,
`INTERNALBATCH`, `INTERNALBATCHV2`, and `INTERNALDIGESTV1` are deliberately
excluded. They are replication primitives, need internal authentication and
binary payload rules, and must never be made available by a normal SQL client.

## SQL forms and translation

Each parsed statement becomes a `CacheCommandRequest`; a program with more than
one statement becomes `{command:"BATCH", batch:[...]}` in source order. As
with the existing BATCH command, this is a pipeline, not a transaction. Use
`BEGIN ATOMIC; ...; COMMIT` for the opt-in scalar atomic mode. It validates the
entire supported scalar batch before writes begin; commands such as `INC` that
can fail after mutation are rejected rather than being falsely described as
transactional. With local partitions enabled, every command in an atomic batch
must route to the same partition; cross-partition atomic batches are rejected
before any command is executed.

For a multi-step application session, `BeginSQLTransaction` provides
snapshot-isolated scalar command SQL. `Execute` stages supported scalar
mutations in a private snapshot, `Query` reads that snapshot including its own
writes, and `Rollback` destroys it without touching live data. `Commit` checks
the live mutation epoch while applying the atomic batch; a concurrent write
causes a conflict and publishes none of the staged commands. Transactions do
not accept `INC`, reads through command SQL, or cross-partition writes.

Atomic command programs also accept savepoints. They are resolved while the
program is compiled, before the final atomic batch is executed:

```sql
BEGIN ATOMIC;
INSERT INTO cache (key, value) VALUES ('keep', 'first');
SAVEPOINT before_discard;
INSERT INTO cache (key, value) VALUES ('discard', 'second');
ROLLBACK TO before_discard;
RELEASE SAVEPOINT before_discard;
COMMIT;
```

`SQLTransaction.Savepoint`, `RollbackTo`, and `ReleaseSavepoint` provide the
same behavior for multi-call snapshot transactions.

| SQL form | Existing command request |
| --- | --- |
| `SELECT value FROM cache WHERE key = 'k'` | `GETSTR`, `key='k'` |
| `SELECT exists FROM cache WHERE key = 'k'` | `EXISTS`, `key='k'` |
| `SELECT ttl FROM cache WHERE key = 'k'` | `TTL`, `key='k'` |
| `SELECT dump FROM cache WHERE key = 'k'` | `DUMP`, `key='k'` |
| `INSERT INTO cache (key, value) VALUES ('k', 'v')` | `SETSTR`, `key='k'`, `value='v'` |
| `INSERT INTO cache (key, value, ttl_seconds) VALUES ('k', 'v', 60)` | `SETSTRX`, `key='k'`, `value='v'`, `ttl_seconds=60` |
| `INSERT INTO cache (key, counter) VALUES ('k', 7)` | `SETINT`, `key='k'`, `value='7'` |
| `MERGE INTO cache (...) ... WHEN NOT MATCHED THEN INSERT` | conditional native `SETSTR` or `SETINT` |
| `UPDATE cache SET value = 'v' WHERE key = 'k'` | `SETSTR`, `key='k'`, `value='v'` |
| `UPDATE cache SET value = value + 2 WHERE key = 'k'` | `INC`, `key='k'`, `value='2'` |
| `UPDATE cache SET ttl_seconds = 60 WHERE key = 'k'` | `EXPIRE`, `key='k'`, `ttl_seconds=60` |
| `UPDATE cache SET unix_seconds = 1735689600 WHERE key = 'k'` | `EXPIREAT`, `key='k'`, `unix_seconds=1735689600` |
| `DELETE FROM cache WHERE key = 'k'` | `DEL`, `key='k'` |
| `CALL NAME(field => value, ...)` | `Command=NAME` plus the named request fields |

`CALL` is the lossless form for every listed public command. Its allowed field
names match `CacheCommandRequest`: `key`, `value`, `values`, `subkey`, `pairs`,
`priority`, `ttl_seconds`, and `unix_seconds`. `values` is a JSON array and
`pairs` is a JSON object, written with an explicit JSON literal:

```sql
CALL PUTMAP(key => 'user:1', pairs => JSON '{"name":"ivi","age":32}');
CALL PUSHSLICE(key => 'jobs', values => JSON '["build","verify"]');
CALL PUSHPQ(key => 'jobs', priority => 1, value => 'rebuild');
CALL RANGEFW(key => 'scores:hourly', value => 8, subkey => 13);
```

The compiler accepts positional shorthand for the common scalar calls:
`CALL GET('k')`, `CALL SETSTR('k', 'v')`, `CALL SETINT('k', 7)`,
`CALL INC('k', 2)`, and `CALL DEL('k')`. All other commands remain available
through named fields, so no command loses expressiveness or gains ambiguous
positional rules.

## Grammar

```ebnf
program       = { statement [ ";" ] } ;
statement     = select | insert | merge | update | delete | call ;
select        = "SELECT" ("value" | "exists" | "ttl" | "dump")
                "FROM" "cache" "WHERE" "key" "=" scalar ;
insert        = "INSERT" "INTO" "cache" "(" columns ")"
                "VALUES" "(" scalars ")" ;
insert_select = "INSERT" "INTO" "cache" "(" columns ")" relational_query ;
merge         = "MERGE" "INTO" "cache" "(" columns ")" "VALUES" "(" scalars ")"
                [ "WHEN" [ "NOT" ] "MATCHED" "THEN" ( "INSERT" | "UPDATE" ) ] ;
update        = "UPDATE" "cache" "SET" assignment "WHERE" "key" "=" scalar ;
delete        = "DELETE" "FROM" "cache" "WHERE" "key" "=" scalar ;
returning     = "RETURNING" ( "*" | identifier { "," identifier } ) ;
call          = "CALL" identifier "(" [ arguments ] ")" ;
arguments     = argument { "," argument } ;
argument      = identifier "=>" literal | literal ;
literal       = scalar | "JSON" string ;
scalar        = string | integer | decimal | "NULL" ;
```

Strings use SQL single quotes; write one quote as `''`. SQL identifiers are
case-insensitive. JSON payloads must be valid JSON and are decoded before the
command is sent, so a malformed payload is diagnosed locally.

## Plan regression guards

Use `hatSql.VerifyPlanGuards` in a normal Go test to make a planner regression
fail CI. Each guard executes the query through `EXPLAIN ANALYZE` and requires a
plan node and optional detail substring. This keeps an intended index path
explicit without matching unstable timing or row-count values.

```go
err := hatSql.VerifyPlanGuards(ctx, resolver, hatSql.SQLQueryOptions{}, []hatSql.PlanGuard{{
    Name:        "people by id",
    Query:       "FROM CACHE('people') AS p WHERE p.id = 7 SELECT p.name",
    RequireNode: "INDEX SCAN",
}})
```

On failure the error includes the guard name, missing requirement, and observed
plan nodes, so the test makes the changed access path visible in CI output.

## Diagnostics

Diagnostics include a one-based line and column, the source line, a caret span,
the expected syntax, and a suggestion only when it is unambiguous. Examples:

```text
error: unexpected "FRMO"; expected FROM; did you mean `FROM`?
 --> query:1:14
  |
1 | SELECT value FRMO cache WHERE key = 'name';
  |              ^^^^
```

Unknown `CALL` names and fields use the same nearest-name suggestion. Semantic
errors identify the argument span, for example a non-integer `ttl_seconds` or
an invalid `JSON` payload.

## CLI

```sh
make cli ARGS="sql -query \"SELECT value FROM cache WHERE key = 'name'\""
make cli ARGS="sql -query \"CALL ADDSET(key => 'tags', values => JSON '[\\\"go\\\"]')\""
make cli ARGS='sql -file examples/cache.sql -wire-format json'
```

Exactly one of `-query`, `-file`, or one positional query is required. `-file`
is read as UTF-8. `-wire-format` retains the existing `auto`, `protobuf`, and
`json` behavior.

## Relational queries

Command statements above are intentionally different from relational queries.
They are compiled client-side and retain the exact existing command semantics.
Relational `SELECT` queries execute against immutable row snapshots on the
server and return rows; they never implicitly mutate cache values.

### Sources

| Source | Rows exposed |
| --- | --- |
| `KEYS` | One row per cache entry: `key`, `type`, `ttl_ms`, `on_disk`, `size_bytes`, and `value_preview`. |
| `CACHE('key')` | The JSON value stored at one cache key. A JSON array of objects produces one row per object; a JSON object produces one row with its fields. Scalars and arrays containing non-objects are rejected with an actionable error. |
| `EXTERNAL('name')` | A caller-imported CSV, JSON, or flat Parquet table snapshot provided by an `ExternalSourceResolver`; it never opens a path or URL from SQL text. |
| `VALUES (...)` | Inline rows, primarily for CTEs, tests, and joining query parameters. |
| `WITH name [(columns...)] AS (SELECT ... | VALUES ...)` | A named source scoped to one query. CTEs can reference earlier CTEs. |
| `WITH RECURSIVE name [(columns...)] AS (seed UNION [ALL] recursive_term)` | A named hierarchy/sequence source. Each recursive iteration sees only rows from the previous iteration. |
| `(SELECT ... | FROM ... SELECT ...) AS alias` | An uncorrelated, read-only derived-table source; it can appear in `FROM` or a join. |

`CACHE('key')` makes application-owned JSON cache values directly queryable
without mirroring them into a second relational store. `KEYS` supplies the
metadata/index view for key discovery and administration.

Each HatTrie query holds one read snapshot across all of its `CACHE` and
`KEYS` sources. Every resolver is memoized per query, so repeated source
references use the same resolved rows; individual HatTrie reads remain
concurrency-safe without holding its lock across query execution.

Each non-recursive CTE is evaluated once, materialized in query-local state,
and reused by every later reference, including self-joins. Recursive CTEs use
their separately documented frontier iteration, depth budget, and optional
cycle detection rather than re-executing the seed for each reference.

### Prepared-query cache

Repeated dashboard queries can reuse an immutable parsed template while still
binding fresh parameter values for every request. `ExecuteSQLQueryParameters`,
`ExecuteSQLQueryPage`, and row streaming use the bounded default cache; supply
your own cache when its lifetime or capacity should be explicit:

```go
prepared := hatriecache.NewSQLPreparedQueryCache(128)
result, err := hatriecache.ExecuteSQLQueryParameters(ctx,
    "FROM CACHE($1) AS u WHERE u.team_id = $2 SELECT u.id, u.name",
    resolver,
    []interface{}{"users", int64(10)},
    hatriecache.SQLQueryOptions{PreparedCache: prepared},
)
```

The cache stores only parsed templates, never resolved rows or parameter
values. Each execution deep-copies the template before binding, so concurrent
requests cannot observe another request's `CACHE($n)`, `VALUES`, predicate,
join, subquery, window, or pagination state. `PreparedCache.Stats()` exposes
entry, hit, and miss counts. A cache with capacity zero disables storage; a
full cache evicts the least-recently-used template.

### Typed prepared queries

`PrepareSQLQuery` adds an explicit schema to the same immutable plan cache.
It requires exactly one declaration for every positional slot through the
largest `$n`, validates values before any source is resolved, and converts
values using the SQL field rules. Supported types are `ANY`, `TEXT`, `NUMBER`,
`INTEGER`, `DECIMAL`, `BOOLEAN`, `DATE`, `TIMESTAMP`, and `JSON`. Parameters
are non-NULL by default; set `Nullable: true` for a SQL NULL parameter.

```go
prepared, err := hatriecache.PrepareSQLQuery(
    "SELECT name FROM CACHE('users') WHERE score >= $1 AND active = $2",
    []hatriecache.SQLParameterSpec{
        {Type: hatriecache.SQLParameterInteger},
        {Type: hatriecache.SQLParameterBoolean},
    },
    hatriecache.NewSQLPreparedQueryCache(128),
)
if err != nil {
    return err
}
result, err := prepared.Execute(ctx, resolver, []interface{}{int64(10), true}, hatriecache.SQLQueryOptions{})
```

`DATE` accepts `YYYY-MM-DD`, `TIMESTAMP` accepts RFC3339/RFC3339Nano text or
`time.Time`, and `DECIMAL` accepts decimal text or a numeric Go value. The
schema is copied when prepared and `Parameters()` returns a copy, so callers
cannot modify a query shared by concurrent requests.

### Materialized views

The public `hatSql` package provides an in-process materialized-view registry
for repeated read-only queries. Each definition declares the cache keys that
invalidate it. `RefreshChanged` reruns only views whose dependency list
intersects the changed keys, then atomically replaces their snapshots. A query
failure preserves every last known-good snapshot.

```go
views := hatSql.NewMaterializedViews()
_, err := views.Create(ctx, hatSql.MaterializedViewDefinition{
    Name:         "active_users",
    Query:        "FROM CACHE('users') WHERE active = true SELECT id, name",
    Dependencies: []string{"users"},
}, resolver, hatSql.QueryOptions{})
if err != nil {
    return err
}

// Call this after a successful update of the users source.
statuses, err := views.RefreshChanged(ctx, []string{"users"}, resolver, hatSql.QueryOptions{})
if err != nil {
    return err
}
view, ok := views.Get("active_users")
_ = statuses
_ = view
_ = ok
```

Dependencies are deliberately explicit rather than inferred from SQL text, so
the registry works equally with `CACHE`, `VALUES`, CTE, and application-defined
resolver sources. This is incremental invalidation and recomputation, not
row-delta maintenance: a changed dependency reruns the affected view query.
`Get` returns an independent snapshot, safe for callers to inspect or modify.

### Query-result subscriptions

`hatSql.QuerySubscriptions` publishes a new snapshot when an explicitly
declared dependency changes and the query rows actually differ. It does not
install background polling or write hooks: call `NotifyChanged` after a
successful application update. This keeps cache writes independent from query
execution and lets applications define their own update transaction boundary.

```go
subscriptions := hatSql.NewQuerySubscriptions(1) // one coalesced latest update
subscription, err := subscriptions.Subscribe(ctx, hatSql.QuerySubscriptionDefinition{
    Query:        "FROM CACHE('users') WHERE active = true SELECT id, name",
    Dependencies: []string{"users"},
}, resolver, hatSql.QueryOptions{})
if err != nil {
    return err
}
if err := subscriptions.NotifyChanged(ctx, []string{"users"}, resolver, hatSql.QueryOptions{}); err != nil {
    return err
}
for update := range subscription.Updates() {
    consume(update.Result)
}
```

The initial result is available from `Snapshot()` at revision one. Updates are
coalesced when a consumer is slow, so each subscriber receives the newest
state without an unbounded queue. `NotifyChanged` evaluates every affected
query before publishing any of them; a query error preserves all last
known-good snapshots. `Close` removes a subscription and closes its channel.

### External CSV, JSON, and Parquet tables

`hatSql.ExternalTables` imports caller-supplied CSV, JSON, or Parquet bytes into named,
immutable snapshots. Query them with `EXTERNAL('name')`; SQL never receives a
path or URL, so this facility cannot read arbitrary local files or make network
requests. CSV requires a unique non-empty header row and preserves cells as
text. JSON accepts an object or an array of objects and preserves JSON values.

```go
tables := hatSql.NewExternalTables()
if err := tables.ImportCSV("events", []byte("id,state\n1,open\n")); err != nil {
    return err
}
result, err := hatSql.ExecuteQueryParameters(ctx,
    "FROM EXTERNAL('events') AS event WHERE event.state = 'open' SELECT event.id",
    tables, nil, hatSql.QueryOptions{})
if err != nil {
    return err
}
csvBytes, err := tables.ExportCSV("events")
_ = result
_ = csvBytes
```

`ImportJSON`, `ExportJSON`, `ImportCSV`, `ExportCSV`, `ImportParquet`, and
`ExportParquet` all clone input and output rows. Parquet import accepts flat
scalar columns; nested/repeated schemas are rejected explicitly. Parquet export
uses optional UTF-8 columns, so arbitrary row values round-trip as text and can
be cast in SQL. Importing the same name replaces its complete snapshot, so a
concurrent query observes either the old table or the new one, never a partial
import.

### Full-text token indexes

For whole-token text search, create an optional index and use
`CONTAINS(field, 'token words')`. Both the index and query normalize text to
case-insensitive Unicode letter-or-number tokens. Every query token must be
present; punctuation and duplicate words are ignored. This differs from
case-sensitive substring `LIKE` and intentionally avoids stemmers, prefixes,
and fuzzy matching.

```go
if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
    return err
}
```

```sql
FROM CACHE('articles') AS article
WHERE CONTAINS(article.body, 'go cache')
SELECT article.id, article.title;
```

The index stores parsed source rows once and compact row-position posting lists
per token, then intersects the smallest postings first. It is lazily refreshed
when the source cache value changes. The normal `CONTAINS` predicate still runs
after the probe, so the index only reduces work and cannot change results.

Create an optional JSON field index with
`trie.CreateSQLJSONFieldIndex("users", "team_id")`. A matching qualified
filter such as `WHERE users.team_id = 20` or `WHERE users.team_id >= 20` uses
`INDEX SCAN`. A one-source query with one qualified `ORDER BY users.team_id`
can read the same index in order and report `INDEX ORDER SCAN`, avoiding the
final `SORT`; it retains source order for equal values and honors `NULLS FIRST`
or `NULLS LAST`. The same untyped single-source scalar shape can use `QueryRows`
without a `LIMIT`: HatTrie visits the ordered index directly, so it does not
build a source slice or sort candidates. When its single `GROUP BY` is that
exact same field, it also
reports `INDEX GROUP AGGREGATE` instead of building a grouping hash table.
For direct grouped-field projections plus `COUNT`, `SUM`, `AVG`, `MIN`, or
`MAX` over a direct field, that operator is a constant-state streaming
aggregate: it retains no source rows and can therefore run below a group-memory
budget that would reject materialized grouping. HatTrie's ordered JSON index
also exposes this exact shape to `QueryRows`: one untyped `CACHE` source, one
indexed `GROUP BY` field, that same one-field `ORDER BY`, direct group-field
projection, and direct `COUNT`, `SUM`, `AVG`, `MIN`, or `MAX` fields. It visits
the ordered index and retains only the active group's aggregate state, so no
source-row or group slice is created. `HAVING`, windows, distinct, expressions
around aggregates, and representative-row projections deliberately retain the
established materialized group semantics.
For the similarly narrow `SELECT DISTINCT indexed_field … ORDER BY
indexed_field` form, `QueryRows` retains only the preceding canonical field
value: equal index entries are adjacent, including `NULL`, so it emits each
field value once without a membership map.
`ROW_NUMBER`, `RANK`, and `DENSE_RANK` can also stream when their unpartitioned
one-field window order exactly matches that same final indexed `ORDER BY`.
They retain only the current position, rank state, and previous ordering value.
`LAG(expression [, literal_offset [, default]])` can use the same proof with a
fixed-size history ring bounded by its literal offset.
`LEAD(expression [, literal_offset [, default]])` is likewise streamable for
that exact order, with a pending queue bounded by the largest literal offset.
Running `SUM`, `AVG`, `MIN`, and `MAX` windows are also streamable for that
exact order, each with constant aggregate state.
Filters, joins, composite order keys, `DISTINCT`, windows, aliases, and other
shapes deliberately retain the general executor until they have an equally
direct ordering proof.
An equality inner join whose right `CACHE` field is indexed uses
`INDEX JOIN`. Equality `LEFT JOIN` probes the same index while preserving every
unmatched left row. Equality `RIGHT JOIN` probes an index on its left CACHE
source while preserving every unmatched right row (`INDEX RIGHT JOIN`). An indexed equality/range predicate remains selectable
inside an `AND` condition; all remaining predicates are still evaluated.
Likewise, an `INNER` or `LEFT` range join may use one indexed comparison inside
an `ON ... AND ...` condition and reports `RANGE INDEX JOIN`; every candidate
still evaluates the complete `ON` expression before it can match, so additional
predicates and SQL null semantics remain exact.
Indexes refresh automatically when that cache value changes.

`SQLJSONIndexStats` reports total and distinct indexed rows plus deterministic
posting-list skew (`MinRowsPerKey`, `MaxRowsPerKey`, `AverageRowsPerKey`, and a
frequency histogram). For a simple equality index probe, `EXPLAIN ANALYZE`
uses the exact current posting-list count when the resolver provides it; other
resolvers fall back to the average cardinality. The value-estimate API accepts
only a value the caller already supplied and never enumerates indexed values.
The estimate never changes query semantics or hides actual row counts.

`SQLJSONRangeStats(key, field, buckets)` exposes equal-depth histogram buckets
from that same ordered field index, including indexed and NULL row counts.
`SQLJSONRangeEstimate(key, field, operator, value)` returns the exact count for
one `<`, `<=`, `>`, or `>=` comparison. Both refresh lazily from the current
cache value and reuse the range index; they do not allocate a second copy of
the source rows.

For an `AND` of individually indexed equality predicates, the optimizer probes
the available term with the lowest average posting-list estimate first; ties
keep SQL's written order. It still evaluates the entire `WHERE` expression
after probing, and falls back to another available index or a source scan when
statistics or the preferred index are unavailable.

For a recurring equality filter on two or more fields, create a composite
index in its declared field order:

```go
if err := trie.CreateSQLJSONCompositeIndex("users", "team_id", "enabled"); err != nil {
	return err
}
```

`WHERE users.team_id = 20 AND users.enabled = TRUE` then uses the longest
matching composite index as an `INDEX SCAN`; condition order in SQL does not
matter. A pure `AND` of two or more left-to-right equality terms in an `INNER`
or `LEFT JOIN` also probes the composite index and reports `COMPOSITE INDEX
JOIN`. `LEFT JOIN` preserves unmatched rows; a `NULL` join key remains
unmatched under normal SQL null semantics. Composite indexes do not accelerate
ranges, and every predicate condition is still evaluated for correctness.
Use `trie.SQLJSONIndexStats("users", "team_id", "enabled")` to obtain its
refreshed row count and distinct composite-key count. Single-field indexes use
the same stats method with one field.

For a connected chain of two or more equality `INNER JOIN`s, the executor also
uses exact source cardinalities from the current snapshot to start with the
smallest source and greedily add the cheapest connected source. `EXPLAIN
ANALYZE` records this as `JOIN REORDER` with the chosen cardinality order.
All single-column equality `INNER`, `LEFT`, `RIGHT`, and `FULL` joins use a
hash table and report `HASH JOIN`; null-preserving sides are emitted after the
matching probe pass. Cross, non-equality, and base-filtered joins retain their
conservative logical execution path so null preservation and filter pushdown
stay exact.
The final unordered row sequence remains the same deterministic source order
as the written join chain; an explicit `ORDER BY` remains the portable way to
request an output ordering.

### Query grammar and semantics

The relational grammar accepts both conventional SQL order and the more
readable data-flow order. These are equivalent:

```sql
SELECT u.name, t.name AS team
FROM CACHE('users') AS u
LEFT JOIN CACHE('teams') AS t ON u.team_id = t.id
WHERE u.enabled = true
ORDER BY team;

FROM CACHE('users') AS u
LEFT JOIN CACHE('teams') AS t ON u.team_id = t.id
WHERE u.enabled = true
SELECT u.name, t.name AS team
ORDER BY team;
```

The parser is deliberately permissive: `FROM`, joins, `WHERE`, `GROUP BY`,
`HAVING`, `SELECT`, `ORDER BY`, `LIMIT`, and `OFFSET` may appear in either
standard order or the source-first order, once each. `ON` is mandatory for
non-CROSS joins; duplicate or misspelled clauses receive a source diagnostic.

The relational grammar supports these forms in addition to the command SQL:

```sql
WITH active_users AS (
  SELECT id, team_id, name FROM CACHE('users')
  WHERE enabled = true
), teams AS (
  SELECT id, name AS team_name FROM CACHE('teams')
)
SELECT u.name, t.team_name, COUNT(*) AS memberships
FROM active_users AS u
LEFT JOIN teams AS t ON u.team_id = t.id
WHERE u.name IS NOT NULL
GROUP BY u.name, t.team_name
HAVING COUNT(*) > 0
ORDER BY memberships DESC, u.name ASC
LIMIT 100;
```

- [x] `WITH ... AS`, `VALUES`, and terminating `WITH RECURSIVE` CTEs. A
      recursive CTE has one seed plus one `UNION` or `UNION ALL` recursive
      term. `UNION` removes rows seen in prior iterations; `UNION ALL` is
      guarded by the query row limit and should include a terminating filter.
- [x] `FROM` `KEYS`, `CACHE('key')`, CTE, and inline `VALUES` sources.
- [x] Inner, `LEFT [OUTER] JOIN`, `RIGHT [OUTER] JOIN`, `FULL [OUTER] JOIN`,
      and `CROSS JOIN`; `ON` is mandatory except for CROSS JOIN.
- [x] Equality inner joins use a hash join; filters that reference only the
      initial source are applied before an inner/cross-join pipeline.
- [x] `WHERE` with `AND`, `OR`, `NOT`, comparisons, `IS [NOT] NULL`, and `LIKE`.

Comparisons and `LIKE` involving `NULL` evaluate to SQL unknown (`NULL`), not
true or false. `WHERE`/`HAVING` retain only true; `AND` and `OR` use the
standard three-valued truth table. Use `IS NULL` or `IS NOT NULL` for null
tests.

`CASE` supports searched conditions (`CASE WHEN condition THEN value ... END`)
and a simple comparison operand (`CASE value WHEN match THEN result ... END`).
`ELSE` is optional and otherwise produces null. Only the selected `THEN` or
`ELSE` expression is evaluated, so a non-selected branch cannot raise a
conversion or function error.

Numbers compare numerically across integer/decimal literals. Non-null literal
comparisons between incompatible types (for example `1 = '1'`) are rejected
locally with both type names and a source span, rather than silently coercing
them to strings.

Text comparisons and `ORDER BY` use case-sensitive UTF-8 binary collation:
`'Z' < 'a' < 'é'`. Locale-specific or case-insensitive collations are not
silently selected.

Use `NULLS FIRST` or `NULLS LAST` after an `ORDER BY` expression whenever null
placement must be explicit, for example `ORDER BY completed_at DESC NULLS
LAST`. The same rule applies inside a window `OVER (... ORDER BY ...)` clause.
Omitting it retains the engine's existing comparison order.

Use `TIMESTAMP '2026-08-22T09:00:00Z'` for an RFC3339 instant. Timestamp
literals compare chronologically and reject malformed values with a source
span and an RFC3339 example.

Use `DATE '2026-08-22'` for a calendar date. Dates validate the calendar,
serialize as `YYYY-MM-DD`, and compare in chronological order.
- [x] Projection with `*`, qualified columns, aliases, literals, and aggregate
      expressions.
- [x] Searched and simple `CASE` expressions with lazy selected-branch
      evaluation and optional `ELSE`.
- [x] `GROUP BY` and `HAVING` with `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
- [x] `ORDER BY ... ASC|DESC`, `LIMIT`, and `OFFSET`.
- [x] `SELECT DISTINCT` after projection, before `ORDER BY`/`LIMIT`.
- [x] `ROW_NUMBER`, `RANK`, `DENSE_RANK`, running `SUM`, `LAG`, and `LEAD`
      windows with `OVER (PARTITION BY ... ORDER BY ...)`. `LAG`/`LEAD`
      accept an optional non-negative integer offset and default value.
- [x] `UNION` (deduplicating) and `UNION ALL` (preserving duplicates) between
      queries with the same projected column names and order.
- [x] `INTERSECT` and `EXCEPT` with SQL set (deduplicating) semantics.
- [x] `EXPLAIN` for a stable, source-free physical plan and `EXPLAIN ANALYZE`
      for one measured execution plus final output statistics.
- [x] Uncorrelated derived-table subqueries in `FROM` and joins.
- [x] Stable source/VALUES/KEYS order before an explicit `ORDER BY`.
- [x] A 100,000-row source/join limit prevents accidental cross-join explosions.

Parenthesized boolean precedence and arithmetic expressions are supported.
Correlated subqueries remain explicitly out of scope rather than accepted
incorrectly. Recursive CTEs support direct self-reference only; mutual
recursion and recursive terms with more than one set operation are rejected
with an actionable diagnostic.

`SQLQueryOptions.MaxRecursionDepth` optionally limits recursive-term
expansions; zero leaves recursion governed by the normal row and timeout
budgets. This is separate from `MaxRows`, which bounds total generated rows.
Set `SQLQueryOptions.DetectRecursiveCycles` to reject an already-produced
`UNION ALL` row with a clear cycle diagnostic.

Recursive CTEs may expose breadth-first traversal and non-terminating-cycle
information directly in their rows:

```sql
WITH RECURSIVE walk(node, depth) AS (...)
SEARCH BREADTH FIRST BY node SET search_order
CYCLE node SET is_cycle
FROM walk SELECT node, depth, search_order, is_cycle;
```

`SEARCH` sorts each recursion level by its `BY` columns and assigns a
one-based `search_order`. `CYCLE` marks a repeated key as `TRUE`, returns that
row once, and does not use it as another recursion frontier. The option-based
cycle detector remains available when repeated rows should be an error.

The server returns ordinary JSON for the CLI and SDK. A query error uses the
same span diagnostics as command SQL.

### Execution budgets and cancellation

Use `ExecuteSQLQueryContext` when executing in-process queries that need a
deadline or resource limits. `MaxRows` defaults to 100,000; the other limits
are opt-in. `MaxJoinWork` bounds nested/hash join work, while byte budgets
bound materialized grouping, sorting, and returned rows. Cancellation is
checked throughout execution and returns the original context error.

```go
result, err := hatriecache.ExecuteSQLQueryContext(ctx, sql, resolver,
    hatriecache.SQLQueryOptions{
        Timeout:        2 * time.Second,
        MaxJoinWork:    1_000_000,
        MaxResultBytes: 8 << 20,
        MaxSortBytes:   16 << 20,
        MaxGroupBytes:  16 << 20,
        MaxSetBytes:    16 << 20,
    })
```

For a query whose final `ORDER BY`, direct grouped aggregate, or distinct set
operation is larger than its in-memory budget, callers can explicitly permit
bounded spill runs. Set `SpillDirectory` (a caller-owned writable temporary
directory) and `MaxSpillBytes`, then set the operator-specific byte limit.
`MaxSortBytes`, `MaxGroupBytes`, and `MaxSetBytes` remain the maximum one
in-memory run or set-state budget; without both spill settings, exceeding an
enabled budget still fails safely as before.

```go
result, err := hatriecache.ExecuteSQLQueryContext(ctx, sql, resolver,
    hatriecache.SQLQueryOptions{
        MaxSortBytes:   16 << 20,
        MaxSetBytes:    16 << 20,
        SpillDirectory: "/var/tmp/hatrie-sql",
        MaxSpillBytes:  512 << 20,
    })
```

The executor evaluates every `ORDER BY` expression before serializing a run,
preserves SQL values (including `DATE`, `DECIMAL`, timestamps, and nested JSON)
and uses the original row ordinal to keep equal keys stable. It merges at most
32 runs at once, performs further bounded merge passes when needed, deletes
all temporary files on success or failure, and reports `EXTERNAL SORT` with
its live spill bytes and final-run count in `EXPLAIN ANALYZE`.

For the monitoring endpoint, configure the policy on `MonitoringOptions` so
request JSON cannot relax it. `SQLRateLimiter` is a separate token bucket for
read-only SQL: authenticated requests are bucketed by their authenticated
token, otherwise by remote address. This avoids letting expensive dashboard
queries consume the write-command quota.

```go
handler := hatriecache.NewMonitoringHandler(trie, hatriecache.MonitoringOptions{
    SQLQueryOptions: hatriecache.SQLQueryOptions{
        Timeout:        2 * time.Second,
        MaxRows:        100_000,
        MaxJoinWork:    1_000_000,
        MaxResultBytes: 8 << 20,
        MaxSortBytes:   16 << 20,
        MaxGroupBytes:  16 << 20,
        MaxSetBytes:    16 << 20,
        SpillDirectory: "/var/tmp/hatrie-sql",
        MaxSpillBytes:  512 << 20,
    },
    SQLRateLimiter: hatriecache.NewRateLimiter(30, time.Minute),
})
```

The same configuration also lets `ExecuteSQLQueryRows` / NDJSON stream an
otherwise unbounded scalar direct `CACHE` or `VALUES` `ORDER BY`: it reads the
source into bounded runs, then sends the final merge straight to the callback
without building a result-row slice. Indexed ordering and finite top-N queries
keep using their cheaper direct-stream and heap paths respectively.

With `MaxSetBytes` under that same spill configuration, row streaming also
accepts scalar direct `CACHE` or `VALUES` `SELECT DISTINCT` queries without an
`ORDER BY`. It writes key-sorted bounded runs, selects the first source ordinal
for every projected-row identity, and merges those ordinals back to the callback.
This preserves ordinary `DISTINCT` first-occurrence order plus `OFFSET`/`LIMIT`
without holding a source, membership set, or result-row slice. Chained set
expressions use the same bounded external stages in the parser's existing
right-associated order; each nested stage is read back from ordinal-sorted
spill runs rather than materializing an intermediate row slice.

Scalar direct-source `UNION`, `INTERSECT`, and `EXCEPT` expressions can use the
same bounded external-set path when every branch is independently streamable
and projects the same columns. It merges projected-row identities on disk, then
restores the existing first-occurrence output order directly to `QueryRows` or
NDJSON. Nested set stages release their consumed runs before the next merge,
so `MaxSpillBytes` bounds all live intermediate files. Joins, typed sources,
and set-level ordering retain their ordinary global execution path.

For `UNION` (without `ALL`), `INTERSECT`, and `EXCEPT`, `MaxSetBytes` similarly
limits distinct-set membership. Once exceeded, a configured spill directory
merges canonical projected-row identities on disk and then restores the
engine's existing first-occurrence result order. `EXPLAIN ANALYZE` reports an
`EXTERNAL SET` operator with its live spill bytes and final-run count. The
individual set-operation branches still use their ordinary materialized query
semantics; only the global membership phase is spilled.

The same spill directory and disk budget can also execute a bounded direct
grouped aggregate when `MaxGroupBytes` is set. The supported shape is one
direct `GROUP BY` field, that same direct `ORDER BY` field, direct group-field
projection, and direct `COUNT`, `SUM`, `AVG`, `MIN`, or `MAX` fields. The
executor writes bounded aggregate-contribution runs and merges them in source
order, preserving floating-point `SUM`/`AVG` behavior while avoiding retained
group rows. For a single untyped `CACHE` source that implements
`SQLStreamSourceResolver` (or for `VALUES`) with an optional source-local,
non-UDF `WHERE`, it also reads input incrementally instead of first building
the source row slice; `EXPLAIN ANALYZE` reports `STREAM SCAN` followed by
`EXTERNAL GROUP AGGREGATE`. `ExecuteSQLQueryRows` / NDJSON emits the final
merged groups straight to its callback rather than constructing a result-row
slice. `HAVING`, windows, distinct, joins, CTEs, typed fields, custom
functions, expressions around aggregates, and other group/order shapes retain
their materialized semantics and group-memory budget.

### Query observations

Materialized and streamed queries can carry an application request ID and emit
one compact, structured completion event without exposing SQL text or result
values. Supply an `Observer`; when `QueryID` is empty the executor assigns a
unique `sql-N` ID. `SlowQueryThreshold` marks events whose total elapsed time
reaches that duration. Events include exact emitted row-payload bytes, output
shape, error text, and an explicit cancellation reason for context cancellation
or deadline expiry. Events also include `Operators`: privacy-safe per-operator
node names, measured input/output rows, exact byte flow where available,
elapsed time, and any available row estimate. Unlike `EXPLAIN ANALYZE`, these counters intentionally omit plan
details, SQL text, cache keys, predicates, and result values; they are safe to
send directly to a metrics or structured-log sink.

Every `ExecuteSQLQueryRows` / NDJSON execution includes a `STREAM OUTPUT`
operator. Its input/output row counts are the successfully delivered callback
rows, its output bytes are the exact emitted row payload, and its elapsed time
covers the whole streamed execution. This remains present on an early callback
failure so sinks can distinguish a failure before any delivered rows from one
after partial output.

```go
result, err := hatriecache.ExecuteSQLQueryParameters(ctx, sql, resolver, args,
    hatriecache.SQLQueryOptions{
        QueryID:            requestID,
        SlowQueryThreshold: 200 * time.Millisecond,
        Observer: hatriecache.SQLQueryObserverFunc(func(event hatriecache.SQLQueryEvent) {
            logger.Info("sql query complete", "query_id", event.QueryID,
                "slow", event.Slow, "result_bytes", event.ResultBytes,
                "error", event.Error)
        }),
    })
_ = result
_ = err
```

### Positional parameters

Pass values separately from SQL with one-based `$1`, `$2`, … placeholders.
They preserve their JSON/Go type and work in expressions, `VALUES`, and
`CACHE($1)` source keys. Missing values and `$0` point to the placeholder in a
Rust-style source diagnostic.

```go
result, err := conn.QueryParameters(ctx,
    "FROM CACHE($1) AS users WHERE users.id = $2 SELECT users.name",
    []interface{}{"users", int64(42)})
```

For HTTP, send the same data as `{"query":"... $1 ...",
"parameters":[...]}`. Values are never interpolated into the SQL text.

### Cursor pagination

`POST /api/sql` accepts `page_size` (maximum 10,000) and an opaque `cursor`.
The response contains `has_more` and `next_cursor`; pass that cursor with the
identical query and parameters for the following page. Cursors are stateless,
bound to the query/parameter payload, and do not create a server-side result
cache. The Go SDK exposes this materialized-page API as `conn.QueryPage`.

### NDJSON row streaming

Set `"stream":true` in a `POST /api/sql` request to receive
`application/x-ndjson`. The server writes one `{"type":"columns",...}`
record, then one `{"type":"row","row":...}` record per projected row,
and finally `{"type":"done","rows":N}`. A runtime failure after response
headers is an `{"type":"error","error":"..."}` terminal record.

Streaming is deliberately exact rather than pretending that every query can
stream: it accepts one `CACHE` or `VALUES` source with scalar `WHERE`,
projection, `OFFSET`, and `LIMIT`, plus a chain of indexed equality `INNER` or
`LEFT` CACHE joins. A no-join typed `CACHE` source is also streamed: every row
is validated and converted before relational evaluation, and the scanner drains
after a result `LIMIT` so a later malformed declared field produces the same
source-spanned diagnostic as materialized execution. Typed sources with joins
remain rejected because indexed probes have not yet been proven to retain that
full validation contract. Registered scalar custom functions are also valid in
that `WHERE` and projection subset: the executor evaluates a one-row function
batch at a time, retaining no result-row slice and preserving the function's
normal type and source diagnostics. A source-only query with a finite `ORDER BY … LIMIT`
(optionally with `OFFSET`) uses a stable bounded top-N heap: it retains at most
`LIMIT + OFFSET` candidates, then emits sorted rows after its source is read.
An unbounded scalar `CACHE` or `VALUES` order can instead stream its final
bounded external-sort merge when `MaxSortBytes`, `SpillDirectory`, and
`MaxSpillBytes` are configured. Those ordered subsets exclude joins, most
grouping, windows, sets, distinct, typed schemas, and custom ordering keys.
Registered scalar functions remain valid in their `WHERE` and projection
expressions, evaluated in bounded one-row batches. The
direct indexed grouped-aggregate form described above is the grouping exception.
It also supports a global, no-join selection made only of direct `COUNT`, `SUM`,
`AVG`, `MIN`, and `MAX` expressions; those keep constant state and emit one
final row without retaining source rows. It
also streams an unbounded, direct one-field indexed `ORDER BY` over one untyped
`CACHE` source; that proof includes its scalar `WHERE`, projection, `OFFSET`,
and `LIMIT`. It still rejects CTEs, other grouped aggregates, unbounded ordering
without such an index proof, most windows,
set operations that need global membership (`UNION`, `INTERSECT`, and `EXCEPT`),
`DISTINCT` outside the configured direct scalar external-set shape, typed JSON
schemas inside global operators, and custom ordering keys inside those global
operators until each has a bounded-memory streaming operator. `UNION ALL` is
the exception: when every
branch is independently streamable and projects the same columns, rows stream
left-to-right with each branch's own `WHERE`, `OFFSET`, and `LIMIT` semantics.
It also streams unpartitioned, unordered windows with the default running
frame: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, and numeric `SUM`/`AVG`/`MIN`/`MAX`.
`LAG(expression [, literal_offset [, default]]) OVER ()` also streams with a
fixed-size history bounded by its offset. `LEAD` with that same unpartitioned
literal-offset shape streams with a pending queue bounded by its offset.
Partitioned, ordered, framed, or dynamic-offset `LAG`/`LEAD` still require a
global operator.
`QueryRows[T]` uses this NDJSON path and closes the response immediately when
its callback returns an error.

Streaming acquires the same resolver snapshot lock as materialized and paged
queries, and releases it after the final callback or error. A streamed source
therefore cannot observe a different cache snapshot halfway through one query.

### `EXPLAIN` and `EXPLAIN ANALYZE`

Prefix any relational query with `EXPLAIN` to inspect its plan without reading
`CACHE` or `KEYS` sources and without running SQL UDFs:

```sql
EXPLAIN
FROM CACHE('users') AS u
WHERE u.enabled = true
SELECT u.name
ORDER BY u.name
LIMIT 20;
```

The returned `plan` and table rows contain `node`, `detail`, and, where known
without reading a source, `estimated_rows` (currently inline `VALUES` and
`VALUES` CTEs). A simple field-equality join is an `EQUALITY JOIN` whose detail
states that it is eligible for `HASH JOIN`; a multi-field equality join also
notes potential `COMPOSITE INDEX JOIN` use. This is a static capability report,
not a promise to read a live index: `EXPLAIN ANALYZE` reports the operator that
was actually selected and executed. Other plan nodes include scans, joins,
filters, aggregation, projection, set operations, sorting, and pagination.

Use `EXPLAIN ANALYZE` to execute the query once. It returns the plan plus
`stats`: total `elapsed_ns`, `output_rows`, `output_columns`, exact
row-payload `result_bytes`, and `plan_steps`.
Each executed scan, join, filter, aggregation, projection, distinct, sort,
pagination, and set-operation plan step also carries `actual_input_rows`,
`actual_output_rows`, and its own `elapsed_ns`. Core row-carrying operators
also report exact logical `actual_input_bytes` and `actual_output_bytes` (JSON
payload bytes, excluding aliases and plan metadata). Index scans with an estimate
also carry signed `estimate_error_rows` (`actual − estimated`) and, for a
nonzero estimate, `estimate_error_percent`. A positive value identifies an
underestimated posting list and a negative value an overestimate. When a
conjunction offers several estimated equality-index probes, an `INDEX
CANDIDATES` step lists each candidate, its estimated rows, its deterministic
`estimated_cost` (one index lookup plus those estimated rows to check), the
selected probe, and explicit rejection such as `index unavailable`; its
input/output counts are candidate/selected counts. Its final `ANALYZE` result
row repeats the measured output-row count and total elapsed time for
table-oriented clients. Source estimates remain absent when they would require
reading or guessing about a cache source.

## Go SDK

Go does not permit a method with its own type parameters, so the closest legal
and idiomatic form is a generic function plus an optional non-generic method:

```go
conn := hatriecache.NewSQLConn("https://cache.example", "bearer-token")
n, err := hatriecache.QueryRows[Row](ctx, conn, sql, func(row Row) error {
    if row.ID == wantedID {
        return errStop // stop early; QueryRows returns this callback error
    }
    return nil
})
```

- [x] Add `/api/sql` with normal JSON responses.
- [x] Add `SQLConn.Query` for materialized results.
- [x] Add generic `QueryRows[T]` with callback early exit.
- [x] Add cancellation through `context.Context`, bounded error bodies, and
      server/client integration tests.

## Custom functions (last phase)

Custom functions are intentionally not selected or implemented until the query
engine, endpoint, and SDK are verified. Before adding them:

- [x] Benchmark Go-native built-ins and viable embedded runtimes on a
      precompiled scalar row predicate (`row.age > 10 && row.score < 9`):
      Go native 1.315 ns/op, GopherLua 120.7 ns/op / 0 allocs, Goja JavaScript
      374.9 ns/op / 3 allocs, and Starlark 768.0 ns/op / 12 allocs.
- [x] Benchmark large-row LuaJIT marshaling and the requested WebAssembly
      runtimes; results and the decision are in [`UDF.md`](UDF.md).
- [x] Use a bounded native `LANGUAGE GO` expression runtime rather than copying
      every row through cgo into LuaJIT.
- [x] Reject arbitrary Go execution, imports, loops, host calls, and function
      calls; only a typed single return expression is accepted.
- [x] Report source/type failures with function-source diagnostics.
- [x] Add `CREATE FUNCTION ... LANGUAGE GO AS 'return expression'` routing to
      `hatrie-cli sql` and `POST /api/sql/functions`.
- [x] Persist normalized registered function definitions across process
      restart with `-sql-functions-path=/var/lib/hatrie-cache/sql-functions.json`.
- [x] Define and implement a numeric `LANGUAGE WASM` ABI and sandboxed
      `LANGUAGE JS` compiler-to-Wasm path; see [`UDF.md`](UDF.md) for limits,
      installation, benchmarks, and executable tests.

When `-sql-functions-path` is configured on `hatrie-cache`, every successful
`POST /api/sql/functions` registration atomically rewrites a deterministic JSON
definition file. Startup reloads and recompiles every definition before the
server begins accepting requests. A malformed file or a function that no longer
compiles fails startup with the specific function index and source diagnostic;
the server never silently starts with missing functions. The file contains only
the user-provided definition metadata and source, never compiled Wasm or Go
runtime state. Leave the flag empty for the previous in-memory-only behavior.

## Implementation checklist

- [x] Add SQL lexer with source locations and SQL string escaping.
- [x] Add parser for SELECT, INSERT, UPDATE, DELETE, CALL, and statement lists.
- [x] Add command compiler and public-command allowlist.
- [x] Add suggestion and Rust-style diagnostic formatter.
- [x] Add parser/compiler tests for scalar forms, every documented public
      `CALL` name, batches, internal-command rejection, and diagnostics.
- [x] Add `hatrie-cli sql` flags, command/relational/function routing, and HTTP
      execution tests.
- [x] Document the command inventory, lossless translations, relational SQL
      sources, SDK, and UDF decision in this document and `UDF.md`.
- [x] Implement relational query parsing, planning, and snapshot execution.
- [x] Implement `/api/sql` and the `SQLConn` / generic `QueryRows` SDK.
- [x] Verify focused SQL/CLI tests and the full `make test` suite.
- [x] Commit and push directly to `master` (`21b6ead`, `79d8613`, `16cb593`).
