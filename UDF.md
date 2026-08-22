# SQL UDF runtimes

`CREATE FUNCTION` defines a pure, scalar function for use in a read-only SQL
query. A function receives a batch of rows and returns one result per row; it
cannot mutate Hatrie, access a network or filesystem, import packages, or
retain query state.

```sql
CREATE FUNCTION eligible(age INTEGER, penalty INTEGER, disabled BOOLEAN)
LANGUAGE GO AS 'return !disabled && (age + 2) * 3 >= 30 && penalty % 2 == 0';
```

The exact runnable GO examples are covered by
[`TestSQLGoFunctionSupportsArithmeticGroupingAndUnaryNot`](sql_function_test.go)
and [`TestExecuteSQLQueryUsesGoFunctionInWhereAndSelect`](sql_function_test.go).
The LuaJIT batch example is covered by
[`TestSQLLuaFunctionVectorizedBatch`](sql_lua_luajit_test.go).

## Runtime matrix

| Language | Status | Expression / ABI | Batch crossing and copies | Safety and portability |
| --- | --- | --- | --- | --- |
| `GO` | Available by default | Restricted, Go-like `return expression`; comparisons, `&&`, `||`, `!`, arithmetic, grouping and `IS [NOT] NULL`. No calls, loops, allocation, imports, or field access. | Native Go values; one result slice allocation per batch. | Pure, bounded evaluator; runs anywhere Go runs. This is the default choice. |
| `LUA` | Available only in a binary built with `-tags luajit` and a LuaJIT development library | One Lua `return expression`, such as `return (not disabled) and score * 2 + 1 or 0`. | One Go→Lua table and one Lua→Go result table per batch. There is one Lua call per batch, not per row. | State has **no standard libraries**: no `io`, `os`, `package`, `debug`, or FFI. CGO and LuaJIT are required. |
| `WASM` | Available by default | Base64 Wasm module exporting the SQL function name; `INTEGER` = `i64`, `NUMBER` = `f64`, `BOOLEAN` = `i32`, with exactly one numeric result. | One Wazero call per row. The current scalar ABI deliberately has no linear-memory text/JSON/vector marshaling. | CGO-free and sandboxed; imports must instantiate successfully. |
| JavaScript via `fastschema/qjs` | Evaluated, not registered | QJS is a QuickJS-in-Wazero runtime, but not yet safe enough to offer arbitrary UDF source. | Prototype vector bridge: 277–322 ns/row for 10K numeric rows after setup; cold runtime + compile: 538 ms. | CGO-free/MIT, but v0.0.6 enables QuickJS standard/OS modules and exposes a `MaxExecutionTime` option without wiring a QuickJS interrupt handler. |
| arbitrary Go / Lua / JS source | Never supported | — | — | Executing host-language source would expose process capabilities or unbounded execution. |

Lua source is rejected by a normal build with a specific rebuild instruction;
it is never silently run through a different interpreter. An invalid value or
runtime failure is returned as an SQL function diagnostic, not hidden or
converted to a result.

## Values, SQL sources, and language compatibility

The SQL layer is intentionally relational. It does not pretend that every
Hatrie data structure is a table: application JSON and cache metadata are the
current row sources. This table is a compatibility contract, not a roadmap.

| Hatrie value / SQL shape | How it enters a query | SQL representation | `GO` UDF access | `LUA` UDF access | `WASM` / JS access | Executable coverage |
| --- | --- | --- | --- | --- | --- | --- |
| `NULL` | `VALUES`, JSON object field, or missing left-join field | `nil` | `ANY`; test `x IS NULL`. | Lua `nil`; returning it preserves SQL null. | Not representable in the numeric ABI. | [`TestExecuteSQLQueryAggregateLimitOffsetAndNullMatrix`](sql_production_test.go) |
| Boolean | `VALUES` or JSON object field | Go `bool` | Declare `BOOLEAN`; use `!enabled`, `&&`, `||`. | Lua boolean; use `not`, `and`, `or`. | Declare `BOOLEAN`; Wasm receives `i32` 0/1 and returns numeric `i32`. | [`TestSQLGoFunctionSupportsArithmeticGroupingAndUnaryNot`](sql_function_test.go), [`TestSQLLuaFunctionVectorizedBatch`](sql_lua_luajit_test.go), [`TestSQLWASMFunctionNumericABI`](sql_wazero_test.go) |
| Integer | `VALUES`, `KEYS.ttl_ms` / `size_bytes`, or an integer-like JSON field | `int64` from SQL literals/metadata; JSON numbers decode as `float64` | Declare `INTEGER` for native integers, `NUMBER` when JSON decimals are possible; use `n % 2`, `n + 1`. | Lua number; integer precision is exact only through LuaJIT's numeric model. | Declare `INTEGER`; Wasm receives signed `i64`. | [`TestSQLGoFunctionSupportsArithmeticGroupingAndUnaryNot`](sql_function_test.go), [`TestSQLWASMFunctionNumericABI`](sql_wazero_test.go) |
| Decimal / JSON number | `VALUES` or JSON object field | `float64` | Declare `NUMBER`; use `price * 1.1`. | Lua number; use `price * 1.1`. | Declare `NUMBER`; Wasm receives `f64`. | [`TestHatTrieSQLSourceDataTypeMatrix`](sql_production_test.go) |
| Text | `VALUES`, `KEYS.key` / `type` / `value_preview`, or JSON object field | Go `string` | Declare `TEXT`; comparisons and `LIKE` are SQL-side. GO UDFs intentionally have no string methods. | Lua string; equality and Lua expression operators only (no standard library). | Not representable in the numeric ABI. | [`TestHatTrieSQLSourceDataTypeMatrix`](sql_production_test.go) |
| Bytes / a non-JSON scalar cache value | Native cache command value | Not a relational row. `CACHE('key')` rejects scalars. | Not reachable through `CACHE`; direct registry calls may accept `[]byte` as `ANY`, but no byte operators exist. | Direct registry calls can pass bytes; returned values must be null, bool, number, or string. | Not representable in the numeric ABI. | [`TestHatTrieSQLSourceDataTypeMatrix`](sql_production_test.go) |
| JSON object | `CACHE('key')` | One `SQLRow`; select each field before calling a UDF. | Field values use the matching scalar row above. Passing a nested object as `ANY` is opaque; GO UDFs cannot index it. | Nested map values are rejected with a clear conversion error. | Selected numeric fields work; nested objects do not. | [`TestHatTrieSQLSourceDataTypeMatrix`](sql_production_test.go), [`TestSQLLuaFunctionReportsUnsupportedValues`](sql_lua_luajit_test.go) |
| JSON array of objects | `CACHE('key')` | One row per object | Field values use the matching scalar row above. | Field values use the matching scalar row above. | Selected numeric fields work. | [`TestHatTrieSQLSourceDataTypeMatrix`](sql_production_test.go) |
| JSON array/scalar at the root | `CACHE('key')` | Rejected: a source must be an object or an array of objects. | Not reachable until it is modeled as rows. | Not reachable until it is modeled as rows. | Not reachable until it is modeled as rows. | [`TestHatTrieSQLSourceDataTypeMatrix`](sql_production_test.go) |
| Map, slice, set, priority queue, filters, bitmaps, radix tree, sketches, Top-K, reservoir, quantile, Fenwick tree | Public cache commands | Not a relational source today; `KEYS` exposes only its metadata row. | Not reachable as a UDF value through SQL. | Not reachable as a UDF value through SQL. | Not reachable as a UDF value through SQL. | Command coverage: [`TestCompileSQLAcceptsEveryDocumentedPublicCallName`](sql_test.go) |
| `KEYS` metadata | `FROM KEYS` | `key`, `type`, `ttl_ms`, `on_disk`, `size_bytes`, `value_preview` | Treat each selected field as text/int/bool above. | Treat each selected field as text/int/bool above. | `ttl_ms` and `size_bytes` work as `i64`; others need a future text ABI. | [`TestHatTrieSQLSourceDataTypeMatrix`](sql_production_test.go) |

Examples of accessing the same row fields in each available UDF language:

```sql
-- JSON: {"age": 18, "disabled": false}; integers stored by VALUES/metadata.
CREATE FUNCTION adult_go(age INTEGER, disabled BOOLEAN)
LANGUAGE GO AS 'return age >= 18 && !disabled';

-- LuaJIT build only. Lua uses `and` / `not`, not Go's `&&` / `!`.
CREATE FUNCTION adult_lua(age INTEGER, disabled BOOLEAN)
LANGUAGE LUA AS 'return age >= 18 and not disabled';

-- WASM source is standard base64, not WAT. The decoded module must export
-- adult_wasm(i64, i32) -> i32: INTEGER is i64 and BOOLEAN is i32 (0 or 1).
CREATE FUNCTION adult_wasm(age INTEGER, disabled BOOLEAN)
LANGUAGE WASM AS '<base64-encoded-wasm-module>';

FROM CACHE('people') AS p
SELECT adult_go(p.age, p.disabled) AS eligible;
```

The exact Wasm binary/ABI example is executable in
[`TestSQLWASMFunctionNumericABI`](sql_wazero_test.go). It exports
`plus_one(i64) -> i64`; use an assembler/compiler to produce the base64 module
instead of hand-editing bytes.

Use a declared type whenever possible. `ANY` permits a value through the
argument gate but does not add object traversal, reflection, or automatic JSON
conversion. This prevents a nested JSON value from acquiring surprising,
runtime-specific semantics.

## Diagnostics

GO parsing and typed runtime errors carry the function name and source span.
For example, a text value passed to an `INTEGER` argument is rendered as:

```text
error: argument "age" expects INTEGER, got TEXT
 --> function eligible:1:8
  |
1 | return age > 10 && score < 9
  |        ^
```

The test cases for source lookup, typed input, arithmetic type mistakes, and
division by zero are in
[`sql_function_test.go`](sql_function_test.go). LuaJIT creation, conversion,
and runtime errors are surfaced through the same `SQLFunctionError` type; its
optional-runtime behavior is tested in
[`sql_lua_luajit_test.go`](sql_lua_luajit_test.go).

## Benchmarks and decision (2026-08-22)

The predicate was `age > 10 && score < 9` on this Linux amd64 host, Go 1.26.5,
and LuaJIT 2.1. Results are guidance for this host, not a universal claim.

| Runtime / bridge | Result | Main overhead |
| --- | ---: | --- |
| Native Go loop | 0.46–0.52 ns/row | Compiler-optimized loop only. |
| Experimental small Go callback VM | 11.65–12.39 ns/row | Callback dispatch. |
| Implemented `GO`, including result vector (1K / 10K / 100K) | 91–95 / 909–980 / 8,403–9,100 ns per batch (84–98 ns/row) | Expression evaluation and output interface slice. |
| LuaJIT loop, data already resident in Lua | 4.07–4.46 ns/row | Excludes any bridge; not representative alone. |
| LuaJIT positional vector bridge (1K / 10K / 100K) | 211.5 / 219.7 / 213.5 ns/row | Constructing argument tables and copying result table. |
| LuaJIT object-shaped vector bridge (1K / 10K / 100K) | 396 / 408 / 464 ns/row | Object-key marshaling in addition to result copying. |
| Wazero scalar Wasm call | 55.2 ns/call | Wasm call boundary only; no text/vector ABI. |
| Wasmtime-go scalar Wasm call | 1,993 ns/call | CGO call boundary. |
| Wasmer-go scalar Wasm call | crashed on this host | Not suitable for this project. |

`GO` remains the default. LuaJIT is useful only when its richer expression
semantics justify its roughly 2–5× host-bridge cost and optional CGO deployment
requirement. Wazero backs the current numeric Wasm ABI. JavaScript will not be
advertised until a runtime has a verified execution interrupt, a minimal host
surface, a stable batch ABI, and source diagnostics.

The aabalke `jit-proof-of-concept` was evaluated but is not embedded: it has no
license file, no module metadata, uses executable-memory/unsafe Go runtime ABI
assumptions, and targets amd64. Copying it into production would be unsafe and
legally unclear. Its experiment can inform a separately licensed and portable
future JIT, but cannot replace this bounded evaluator today.

## References

- [Go-Joker architecture and benchmark caveat](https://github.com/rcarmo/go-joker)
- [Wazero compiler/runtime](https://github.com/wazero/wazero)
- [QJS JavaScript runtime on Wazero](https://github.com/fastschema/qjs)
- [SQLite testing strategy](https://www.sqlite.org/testing.html)
- [PostgreSQL regression tests](https://www.postgresql.org/docs/current/regress.html)
