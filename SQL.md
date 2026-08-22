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
| **Command SQL** | One cache key at a time. It can create, replace, update, expire, and delete cache values. | `SELECT value|exists|ttl|dump FROM cache`, `INSERT`, `UPDATE`, `DELETE`, `CALL` |
| **Relational SQL** | A read-only snapshot of JSON rows or cache metadata. It never changes a cache value. | `SELECT ... FROM CACHE(...)`, `KEYS`, `VALUES`, CTEs, joins, grouping, and related clauses |

Do not confuse `SELECT value FROM cache WHERE key = 'name'` with `SELECT ...
FROM CACHE('users')`: the first is a command-SQL lookup of one cache key; the
second is a relational query over JSON rows held in a cache value.

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
  statement does not undo an earlier successful statement.
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

### Combine query results

```sql
SELECT value FROM VALUES (1), (2) AS left_rows(value)
UNION
SELECT value FROM VALUES (2), (3) AS right_rows(value)
ORDER BY value;
```

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
| Compare time correctly | `WHERE occurred_at >= TIMESTAMP '2026-08-22T09:00:00Z'` or `day > DATE '2026-08-22'` | Chronological typed comparison; malformed literals are rejected locally. |
| Normalize a dynamic field explicitly | `CAST(raw_score AS NUMBER)` | Supports `TEXT`, `NUMBER`, `DECIMAL`, `BOOLEAN`, `DATE`, and `TIMESTAMP`; invalid dynamic values produce a source-spanned error instead of silently becoming NULL. |
| Validate a JSON source schema | `FROM CACHE('users') AS u(id INTEGER, joined_on DATE)` | Validates and converts declared non-null fields before relational evaluation; a bad row identifies its cache key, row, field, expected type, and source span. |
| Inspect a plan | `EXPLAIN FROM VALUES (1) AS rows(value) SELECT value` | Returns a plan without reading cache sources. |
| Measure one plan | `EXPLAIN ANALYZE FROM ... SELECT ...` | Executes once and returns plan steps plus elapsed/output statistics. |
| Avoid text interpolation | `... WHERE u.id = $1` with separate parameters | Parameters keep their JSON/Go type and are not concatenated into SQL. |
| Page results | `POST /api/sql` with `page_size` and returned `cursor` | Reads the next page from the same query/parameter payload; no mutation. |

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
with the existing BATCH command, this is a pipeline, not a transaction.

| SQL form | Existing command request |
| --- | --- |
| `SELECT value FROM cache WHERE key = 'k'` | `GETSTR`, `key='k'` |
| `SELECT exists FROM cache WHERE key = 'k'` | `EXISTS`, `key='k'` |
| `SELECT ttl FROM cache WHERE key = 'k'` | `TTL`, `key='k'` |
| `SELECT dump FROM cache WHERE key = 'k'` | `DUMP`, `key='k'` |
| `INSERT INTO cache (key, value) VALUES ('k', 'v')` | `SETSTR`, `key='k'`, `value='v'` |
| `INSERT INTO cache (key, value, ttl_seconds) VALUES ('k', 'v', 60)` | `SETSTRX`, `key='k'`, `value='v'`, `ttl_seconds=60` |
| `INSERT INTO cache (key, counter) VALUES ('k', 7)` | `SETINT`, `key='k'`, `value='7'` |
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
statement     = select | insert | update | delete | call ;
select        = "SELECT" ("value" | "exists" | "ttl" | "dump")
                "FROM" "cache" "WHERE" "key" "=" scalar ;
insert        = "INSERT" "INTO" "cache" "(" columns ")"
                "VALUES" "(" scalars ")" ;
update        = "UPDATE" "cache" "SET" assignment "WHERE" "key" "=" scalar ;
delete        = "DELETE" "FROM" "cache" "WHERE" "key" "=" scalar ;
call          = "CALL" identifier "(" [ arguments ] ")" ;
arguments     = argument { "," argument } ;
argument      = identifier "=>" literal | literal ;
literal       = scalar | "JSON" string ;
scalar        = string | integer | decimal | "NULL" ;
```

Strings use SQL single quotes; write one quote as `''`. SQL identifiers are
case-insensitive. JSON payloads must be valid JSON and are decoded before the
command is sent, so a malformed payload is diagnosed locally.

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

Create an optional JSON field index with
`trie.CreateSQLJSONFieldIndex("users", "team_id")`. A matching qualified
filter such as `WHERE users.team_id = 20` or `WHERE users.team_id >= 20` uses
`INDEX SCAN`; an equality inner join whose right `CACHE` field is indexed uses
`INDEX JOIN`. Equality `LEFT JOIN` probes the same index while preserving every
unmatched left row. An indexed equality/range predicate remains selectable
inside an `AND` condition; all remaining predicates are still evaluated.
Indexes refresh automatically when that cache value changes.

For a recurring equality filter on two or more fields, create a composite
index in its declared field order:

```go
if err := trie.CreateSQLJSONCompositeIndex("users", "team_id", "enabled"); err != nil {
	return err
}
```

`WHERE users.team_id = 20 AND users.enabled = TRUE` then uses the longest
matching composite index as an `INDEX SCAN`; condition order in SQL does not
matter. Composite indexes currently accelerate qualified equality predicates,
not ranges or joins, and the full condition is still evaluated for correctness.
Use `trie.SQLJSONIndexStats("users", "team_id", "enabled")` to obtain its
refreshed row count and distinct composite-key count. Single-field indexes use
the same stats method with one field.

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

Numbers compare numerically across integer/decimal literals. Non-null literal
comparisons between incompatible types (for example `1 = '1'`) are rejected
locally with both type names and a source span, rather than silently coercing
them to strings.

Text comparisons and `ORDER BY` use case-sensitive UTF-8 binary collation:
`'Z' < 'a' < 'é'`. Locale-specific or case-insensitive collations are not
silently selected.

Use `TIMESTAMP '2026-08-22T09:00:00Z'` for an RFC3339 instant. Timestamp
literals compare chronologically and reject malformed values with a source
span and an RFC3339 example.

Use `DATE '2026-08-22'` for a calendar date. Dates validate the calendar,
serialize as `YYYY-MM-DD`, and compare in chronological order.
- [x] Projection with `*`, qualified columns, aliases, literals, and aggregate
      expressions.
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
    })
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
cache. The Go SDK exposes this as `conn.QueryPage`; `QueryRows` follows those
cursors automatically with 1,000-row pages, so it does not retain earlier
result pages while invoking its callback.

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
`VALUES` CTEs). Plan nodes include scans, joins, filters, aggregation,
projection, set operations, sorting, and pagination.

Use `EXPLAIN ANALYZE` to execute the query once. It returns the plan plus
`stats`: total `elapsed_ns`, `output_rows`, `output_columns`, and `plan_steps`.
Each executed scan, join, filter, aggregation, projection, distinct, sort,
pagination, and set-operation plan step also carries `actual_input_rows`,
`actual_output_rows`, and its own `elapsed_ns`. Its final `ANALYZE` result row
repeats the measured output-row count and total elapsed time for table-oriented
clients. Source estimates remain absent when they would require reading or
guessing about a cache source.

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
- [ ] Persist registered function definitions across process restart.
- [x] Define and implement a numeric `LANGUAGE WASM` ABI and sandboxed
      `LANGUAGE JS` compiler-to-Wasm path; see [`UDF.md`](UDF.md) for limits,
      installation, benchmarks, and executable tests.

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
