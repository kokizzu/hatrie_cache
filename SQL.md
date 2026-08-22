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
| `(SELECT ... | FROM ... SELECT ...) AS alias` | An uncorrelated, read-only derived-table source; it can appear in `FROM` or a join. |

`CACHE('key')` makes application-owned JSON cache values directly queryable
without mirroring them into a second relational store. `KEYS` supplies the
metadata/index view for key discovery and administration.

Each HatTrie query holds one read snapshot across all of its `CACHE` and
`KEYS` sources, so concurrent writes cannot create a mixed-time join. External
resolvers are memoized per query for repeated source references.

Create an optional equality index for a JSON field with
`trie.CreateSQLJSONFieldIndex("users", "team_id")`. A matching qualified
filter such as `WHERE users.team_id = 20` uses `INDEX SCAN`; indexes refresh
automatically when that cache value changes.

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

- [x] `WITH ... AS` and `VALUES` CTEs.
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
- [x] Projection with `*`, qualified columns, aliases, literals, and aggregate
      expressions.
- [x] `GROUP BY` and `HAVING` with `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
- [x] `ORDER BY ... ASC|DESC`, `LIMIT`, and `OFFSET`.
- [x] `SELECT DISTINCT` after projection, before `ORDER BY`/`LIMIT`.
- [x] `UNION` (deduplicating) and `UNION ALL` (preserving duplicates) between
      queries with the same projected column names and order.
- [x] `INTERSECT` and `EXCEPT` with SQL set (deduplicating) semantics.
- [x] `EXPLAIN` for a stable, source-free physical plan and `EXPLAIN ANALYZE`
      for one measured execution plus final output statistics.
- [x] Uncorrelated derived-table subqueries in `FROM` and joins.
- [x] Stable source/VALUES/KEYS order before an explicit `ORDER BY`.
- [x] A 100,000-row source/join limit prevents accidental cross-join explosions.

Parenthesized boolean precedence and arithmetic expressions are supported.
Correlated subqueries, window functions, and recursive CTEs remain explicitly
out of scope rather than accepted incorrectly.

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
cache. The Go SDK exposes this as `conn.QueryPage`.

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
