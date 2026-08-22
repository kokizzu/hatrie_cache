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
[`TestSQLLuaFunctionVectorizedBatch`](sql_lua_luajit_test.go). The JavaScript
batch, compiler diagnostic, and timeout tests are in
[`sql_javascript_javy_test.go`](sql_javascript_javy_test.go); they run with
`-tags javy` and a deliberately explicit `-sql-js-compiler=/path/to/javy`.

## Runtime matrix

| Language | Status | Expression / ABI | Batch crossing and copies | Safety and portability |
| --- | --- | --- | --- | --- |
| `GO` | Available by default | Restricted, Go-like `return expression`; comparisons, `&&`, `||`, `!`, arithmetic, grouping and `IS [NOT] NULL`. No calls, loops, allocation, imports, or field access. | Native Go values; one result slice allocation per batch. | Pure, bounded evaluator; runs anywhere Go runs. This is the default choice. |
| `LUA` | Available only in a binary built with `-tags luajit` and a LuaJIT development library | One Lua `return expression`, such as `return (not disabled) and score * 2 + 1 or 0`. | One Go→Lua table and one Lua→Go result table per batch. There is one Lua call per batch, not per row. | State has **no standard libraries**: no `io`, `os`, `package`, `debug`, or FFI. CGO and LuaJIT are required. |
| `WASM` | Available by default | Base64 Wasm module exporting the SQL function name; `INTEGER` = `i64`, `NUMBER` = `f64`, `BOOLEAN` = `i32`, with exactly one numeric result. | One Wazero call per row. The current scalar ABI deliberately has no linear-memory text/JSON/vector marshaling. | CGO-free and sandboxed; imports must instantiate successfully. |
| `JS` | Available when the [Javy](https://github.com/bytecodealliance/javy) compiler is installed; provide `SQLFunctionRegistryOptions.JavyPath` or put `javy` on `PATH`. | A JavaScript function body containing `return`, compiled once to a WASI module. Inputs/outputs are a JSON vector of positional rows. | One JSON encode and one JSON decode plus one Wazero/WASI module instance per batch. Static Javy modules add substantial startup/allocation cost. | Javy compilation is isolated in a private temporary directory; execution has no mounted filesystem/network/process capabilities. Wazero `WithCloseOnContextDone(true)` terminates a timed-out module. |
| arbitrary Go source | Never supported | — | — | Host-language Go source would expose process capabilities and unbounded execution. |

Lua source is rejected by a normal build with a specific rebuild instruction;
it is never silently run through a different interpreter. An invalid value or
runtime failure is returned as an SQL function diagnostic, not hidden or
converted to a result.

`LANGUAGE JS` is intentionally compiler-dependent instead of embedding an
in-process JavaScript interpreter. At registration the service invokes Javy
with direct process arguments (never a shell), reads only its generated Wasm,
then removes the compiler directory. At query time, the compiler is not
involved. Each batch has a one-second default deadline, a 16 MiB encoded input
limit, and a 16 MiB output limit; callers can set a different deadline with
`SQLFunctionRegistryOptions.JSExecutionTimeout`.

The GO source is parsed once, validated against declared arguments, and lowered
to a compact post-order stack program before the first row runs. This takes the
small typed-IR idea from Go-Joker without embedding its broad Clojure-like
language surface. The stack is reused for every row in a batch; it is not an
unsafe native-code JIT and cannot gain host-process capabilities.

## Values, SQL sources, and language compatibility

The SQL layer is intentionally relational. It does not pretend that every
Hatrie data structure is a table: application JSON and cache metadata are the
current row sources. This table is a compatibility contract, not a roadmap.

| Hatrie value / SQL shape | How it enters a query | SQL representation | `GO` UDF access | `LUA` UDF access | `WASM` (see JS matrix below) | Executable coverage |
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

### JavaScript value conversion matrix

`JS` has a deliberately separate contract from numeric Wasm. It receives and
returns a JSON vector, so it can operate on text and nested JSON but cannot
silently preserve every Go value. The conversion tests are linked below.

| SQL value | JavaScript value | Result conversion / limit | Executable coverage |
| --- | --- | --- | --- |
| `NULL` | `null` | `null` returns SQL null. | [`TestSQLJavaScriptFunctionVectorizedBatch`](sql_javascript_javy_test.go) |
| `BOOLEAN` | `boolean` | JSON boolean is returned unchanged. | [`TestSQLJavaScriptFunctionVectorizedBatch`](sql_javascript_javy_test.go) |
| `INTEGER` | `number` | Exact only through ±(2<sup>53</sup>−1); larger `int64` inputs are rejected rather than rounded. | [`TestSQLJavaScriptValidationRejectsMissingReturnAndUnsafeInteger`](sql_function_test.go) |
| `NUMBER` | `number` | Finite JSON numbers only; JSON integer output becomes `int64` when representable, otherwise `float64`. | [`TestSQLJavaScriptFunctionVectorizedBatch`](sql_javascript_javy_test.go) |
| `TEXT` | `string` | JSON string is returned unchanged. | [`TestSQLJavaScriptFunctionVectorizedBatch`](sql_javascript_javy_test.go) |
| Nested JSON object/array | object / array | Direct registry arguments are JSON encoded, and JavaScript may traverse or return them. SQL row sources still require object rows. | [`TestSQLJavaScriptValueConversionPreservesJSONShapes`](sql_function_test.go) |
| `[]byte`, NaN, ±Infinity, arbitrary Go object | — | Rejected before execution with a precise argument diagnostic. | [`TestSQLJavaScriptValidationRejectsMissingReturnAndUnsafeInteger`](sql_function_test.go) |

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

-- Javy installed. JavaScript source is a function body, so `return` is required.
CREATE FUNCTION adult_js(age INTEGER, disabled BOOLEAN)
LANGUAGE JS AS 'return age >= 18 && !disabled;';

FROM CACHE('people') AS p
SELECT adult_go(p.age, p.disabled), adult_js(p.age, p.disabled) AS eligible;
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

JavaScript compiler failures are translated back from generated-wrapper lines
to the submitted source and use this formatter too. The integration tests cover
a vector batch, a syntax failure, and a non-terminating `while (true)` body;
the latter returns `JavaScript execution exceeded … batch limit` rather than
hanging a query.

## Benchmarks and decision (2026-08-22)

The predicate was `age > 10 && score < 9` on this Linux amd64 host, Go 1.26.5,
and LuaJIT 2.1. Results are guidance for this host, not a universal claim.

| Runtime / bridge | Result | Main overhead |
| --- | ---: | --- |
| Native Go loop | 0.46–0.52 ns/row | Compiler-optimized loop only. |
| Experimental small Go callback VM | 11.65–12.39 ns/row | Callback dispatch. |
| Implemented stack-IR `GO`, including result vector (1K / 10K / 100K) | 90.6–93.6 µs / 923–949 µs / 8.01–8.50 ms per batch (80–94 ns/row) | 3–4 allocations per batch: result vector plus reused evaluation stack. |
| LuaJIT loop, data already resident in Lua | 4.07–4.46 ns/row | Excludes any bridge; not representative alone. |
| Implemented LuaJIT positional vector bridge (1K / 10K / 100K) | 379–401 / 3.99–4.17 / 40.6–42.7 ms per batch (379–427 ns/row) | Constructing argument tables and copying result table. |
| LuaJIT object-shaped vector bridge (1K / 10K / 100K) | 396 / 408 / 464 ns/row | Object-key marshaling in addition to result copying. |
| Implemented Wazero numeric Wasm ABI (1K / 10K / 100K) | 74–78 µs / 766–792 µs / 7.09–7.72 ms per batch (71–78 ns/row) | One Wasm call per row; Wazero call/result allocations currently dominate. |
| Implemented JavaScript→Javy→Wazero vector ABI (1K / 10K / 100K) | 3.42 / 26.77 / 297.11 ms per batch (3.42 / 2.68 / 2.97 µs/row) | JSON marshaling plus a fresh WASI module instance per batch: 3.8 / 14.2 / 132.5 MiB and 5.4K / 50.4K / 500.5K allocations per batch. |
| Wasmtime-go scalar Wasm call | 1,993 ns/call | CGO call boundary. |
| Wasmer-go scalar Wasm call | crashed on this host | Not suitable for this project. |

The reproducible benchmark functions are
[`BenchmarkSQLGoFunctionBatch`](sql_function_test.go),
[`BenchmarkSQLLuaFunctionBatch`](sql_lua_luajit_test.go), and
[`BenchmarkSQLWASMFunctionBatch`](sql_wazero_test.go), and
[`BenchmarkSQLJavaScriptFunctionBatch`](sql_javascript_javy_test.go). They are run with
`go test -run '^$' -bench ...`; LuaJIT uses `-tags luajit`, while JavaScript
uses `-tags javy -args -sql-js-compiler=/absolute/path/to/javy`. The
"data already resident in Lua" number is included only to show why excluding
marshaling would give a misleading runtime comparison.

`GO` remains the default because it is bounded, rich enough for SQL predicates,
and has minimal batch allocation. LuaJIT is useful only when its richer
expression semantics justify its roughly 4–5× host-bridge cost and optional
CGO deployment requirement. Wazero backs the current numeric Wasm ABI.
JavaScript is the slowest supported option because it safely crosses JSON and
WASI; choose it for JavaScript-specific logic, not simple predicates. The
earlier QJS prototype remains rejected: a 50 ms context deadline did not stop
an infinite loop within five seconds. Javy's generated Wasm did terminate with
Wazero `WithCloseOnContextDone(true)` in the timeout integration test.

The aabalke `jit-proof-of-concept` was evaluated but is not embedded: it has no
license file, no module metadata, uses executable-memory/unsafe Go runtime ABI
assumptions, and targets amd64. Copying it into production would be unsafe and
legally unclear. Its experiment can inform a separately licensed and portable
future JIT, but cannot replace this bounded evaluator today.

## References

- [Go-Joker architecture and benchmark caveat](https://github.com/rcarmo/go-joker)
- [Wazero compiler/runtime](https://github.com/wazero/wazero)
- [Javy JavaScript-to-Wasm compiler](https://github.com/bytecodealliance/javy)
- [QJS JavaScript runtime on Wazero](https://github.com/fastschema/qjs)
- [SQLite testing strategy](https://www.sqlite.org/testing.html)
- [PostgreSQL regression tests](https://www.postgresql.org/docs/current/regress.html)
