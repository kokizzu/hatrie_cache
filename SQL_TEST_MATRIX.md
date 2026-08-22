# SQL production test matrix

This is the verification companion to [SQL.md](SQL.md). It maps supported
behavior to executable tests, following the useful parts of SQLite's fuzzing
strategy and PostgreSQL's regression philosophy: syntax, errors, ordering,
nulls, types, and operational boundaries are tested alongside happy paths.

## Command compiler

| Requirement | Evidence |
| --- | --- |
| Scalar command translations, TTL/expiration forms, positional scalar calls | [`TestCompileSQLProductionScalarMatrix`](sql_production_test.go) |
| Every documented public flat command name | [`TestCompileSQLAcceptsEveryDocumentedPublicCallName`](sql_test.go) |
| Dotted aliases, including every alias-map entry | [`TestCompileSQLDottedCollectionAliases`](sql_production_test.go), [`TestDottedCollectionAliasesNormalizeToExistingCommands`](sql_production_test.go) |
| Internal replication commands cannot be compiled | [`TestCompileSQLRejectsInternalReplicationCommands`](sql_test.go) |
| Duplicate fields, ambiguous scalar forms, mixed call styles, unsafe batch form | [`TestCompileSQLProductionRejectsAmbiguousOrUnsafeForms`](sql_production_test.go) |
| Rust-style lexical/parser diagnostics and suggestions | [`TestFormatSQLDiagnosticSuggestsKeywordAndShowsSourceSpan`](sql_test.go) |

## Relational execution

| Requirement | Evidence |
| --- | --- |
| `KEYS`, `CACHE(object)`, `CACHE(array)`, root scalar rejection, unknown source | [`TestHatTrieSQLSourceDataTypeMatrix`](sql_production_test.go) |
| Source-first syntax, inner/left/cross joins, grouping and ordering | [`sql_query_test.go`](sql_query_test.go) |
| Standard `AND` precedence, `NOT`, `DISTINCT` | [`TestExecuteSQLQueryUsesStandardBooleanPrecedence`](sql_production_test.go), [`TestExecuteSQLQuerySupportsNotAndDistinct`](sql_production_test.go) |
| Nulls, `LIKE`, aggregates, `HAVING`, limit and offset | [`TestExecuteSQLQueryAggregateLimitOffsetAndNullMatrix`](sql_production_test.go) |
| `RIGHT` and `FULL OUTER JOIN`, including unmatched sides | [`TestExecuteSQLQuerySupportsRightAndFullOuterJoin`](sql_production_test.go) |
| `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT` | [`TestExecuteSQLQuerySupportsUnionAndUnionAll`](sql_production_test.go) |
| Derived-table sources | [`TestExecuteSQLQuerySupportsDerivedTableSubqueries`](sql_production_test.go) |
| Duplicate clauses and malformed structure | [`TestExecuteSQLQueryProductionRejectsStructuralErrors`](sql_production_test.go) |
| One Hatrie key has one typed value slot; type prefixes are required | [`TestHatrieTypesShareOneLogicalKeyNamespace`](sql_production_test.go) |

## UDFs, API, and robustness

| Requirement | Evidence |
| --- | --- |
| GO parsing, typed arguments, source spans, arithmetic and divide-by-zero diagnostics | [`sql_function_test.go`](sql_function_test.go) |
| GO UDF use in `WHERE` and `SELECT` | [`TestExecuteSQLQueryUsesGoFunctionInWhereAndSelect`](sql_function_test.go) |
| Optional sandboxed LuaJIT vector batching and conversion rejection | [`sql_lua_luajit_test.go`](sql_lua_luajit_test.go) (`-tags luajit`) |
| Numeric Wazero Wasm ABI, type rejection, and runtime close | [`sql_wazero_test.go`](sql_wazero_test.go) |
| Sandboxed JavaScript→Javy→Wazero vector batching, source spans, and timeout interruption | [`sql_javascript_javy_test.go`](sql_javascript_javy_test.go) (`-tags javy`, explicit compiler path) |
| HTTP SQL/function malformed requests and normal calls | [`sql_http_test.go`](sql_http_test.go), [`TestMonitoringSQLRoutesRejectMalformedRequests`](sql_production_test.go) |
| CLI command, relational query, and function routes | [`cmd/hatrie-cli/sql_test.go`](cmd/hatrie-cli/sql_test.go) |
| Generic Go SDK decode and callback early stop | [`sql_client_test.go`](sql_client_test.go) |
| Parser and executor panic resistance | [`FuzzSQLParsersDoNotPanic`](sql_production_test.go), [`FuzzExecuteSQLQueryDoesNotPanic`](sql_production_test.go) |
| Full suite, race, vet, and package coverage | `make test` (`scripts/verify-go.sh`) |

## Deliberately unsupported

The parser must reject or diagnose these rather than pretend to implement them:

- Correlated subqueries and recursive CTEs.
- Window functions.
- `INTERSECT ALL` and `EXCEPT ALL`.
- SQL writes through relational `SELECT`; cache mutations use the separately
  compiled command SQL forms.
- General text/JSON Wasm ABI. JavaScript is supported through the separately
  documented Javy-to-Wazero batch ABI; see [UDF.md](UDF.md).

The repository's canonical full verification entry point is `make test`.
