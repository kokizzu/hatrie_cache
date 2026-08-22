# SQL CLI support

This document defines the SQL-like client language. It is a client-side
compiler: `hatrie-cli sql` translates command statements to the existing
authenticated `/api/commands` request format. Read-only relational `SELECT`
queries are validated locally and executed through the authenticated
`/api/sql` endpoint against a cache snapshot.

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

`CACHE('key')` makes application-owned JSON cache values directly queryable
without mirroring them into a second relational store. `KEYS` supplies the
metadata/index view for key discovery and administration.

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
- [x] `JOIN` (inner), `LEFT JOIN`, and `CROSS JOIN`; `ON` is mandatory except
      for CROSS JOIN.
- [x] `WHERE` with `AND`, `OR`, comparisons, `IS [NOT] NULL`, and `LIKE`.
- [x] Projection with `*`, qualified columns, aliases, literals, and aggregate
      expressions.
- [x] `GROUP BY` and `HAVING` with `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.
- [x] `ORDER BY ... ASC|DESC`, `LIMIT`, and `OFFSET`.
- [x] Stable source/VALUES/KEYS order before an explicit `ORDER BY`.
- [x] A 100,000-row source/join limit prevents accidental cross-join explosions.

`NOT`, parenthesized boolean precedence, arithmetic expressions, `RIGHT` and
`FULL` joins, `DISTINCT`, `UNION`, and subqueries outside `WITH` are explicitly
out of scope for this first read-only engine rather than accepted incorrectly.

The server returns ordinary JSON for the CLI and SDK. A query error uses the
same span diagnostics as command SQL.

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
- [ ] Define and implement a separately versioned `LANGUAGE WASM` binary ABI
      if users need non-GO UDFs; JavaScript source is not Wasm by itself.

## Implementation checklist

- [x] Add SQL lexer with source locations and SQL string escaping.
- [x] Add parser for SELECT, INSERT, UPDATE, DELETE, CALL, and statement lists.
- [x] Add command compiler and public-command allowlist.
- [x] Add suggestion and Rust-style diagnostic formatter.
- [ ] Add parser/compiler tests before implementation (TDD red stage).
- [ ] Add `hatrie-cli sql` routing, flags, and HTTP execution tests.
- [ ] Document `sql` in README and Makefile CLI examples.
- [ ] Implement relational query parsing, planning, and snapshot execution.
- [ ] Implement `/api/sql` and the streaming Go SDK.
- [ ] Run focused tests, `go test ./...`, `go build ./...`, and formatting.
- [ ] Mark completed boxes with verification evidence and commit/push `master`.
