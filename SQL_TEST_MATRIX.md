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
| Every accepted statement word, selector, mutation field, named-call field, and `JSON` literal marker | Named subtests in [`TestSQLAcceptedKeywordInventory`](sql_function_test.go) |

## Relational execution

| Requirement | Evidence |
| --- | --- |
| `KEYS`, `CACHE(object)`, `CACHE(array)`, root scalar rejection, unknown source | [`TestHatTrieSQLSourceDataTypeMatrix`](sql_production_test.go) |
| Source-first syntax, inner/left/cross joins, grouping and ordering | [`sql_query_test.go`](sql_query_test.go) |
| One multi-join pipeline across `CACHE`, a `VALUES` CTE, a filtered derived `CACHE` source, and inline `VALUES`; verifies inner, left, and cross sequencing with null preservation | [`TestExecuteSQLQueryJoinsMultipleSourceKindsInOnePipeline`](sql_production_test.go) |
| Equality hash join and safe base-source filter pushdown before an inner join | [`TestExecuteSQLQueryPushesBaseFilterIntoHashJoin`](sql_production_test.go) |
| Equality `AND` filters choose the lowest-cardinality available field index while retaining the complete predicate evaluation | [`TestExecuteSQLQueryChoosesMostSelectiveIndexedConjunct`](sql_production_test.go) |
| Connected multi-way equality inner joins choose the smallest current-snapshot source first, expose the decision through `EXPLAIN ANALYZE`, and retain deterministic written-source result order | [`TestExecuteSQLQueryReordersConnectedInnerHashJoinsByCardinality`](sql_production_test.go) |
| Standard `AND` precedence, `NOT`, `DISTINCT` | [`TestExecuteSQLQueryUsesStandardBooleanPrecedence`](sql_production_test.go), [`TestExecuteSQLQuerySupportsNotAndDistinct`](sql_production_test.go) |
| Nulls, `LIKE`, aggregates, `HAVING`, limit and offset | [`TestExecuteSQLQueryAggregateLimitOffsetAndNullMatrix`](sql_production_test.go) |
| RFC3339 `TIMESTAMP` literals, chronological comparisons, and actionable malformed-literal diagnostics | [`TestExecuteSQLQuerySupportsTimestampLiteralsAndDiagnostics`](sql_production_test.go) |
| Calendar-valid `DATE` literals, chronological comparisons, and actionable malformed-literal diagnostics | [`TestExecuteSQLQuerySupportsDateLiteralsAndDiagnostics`](sql_production_test.go) |
| Incompatible non-null literal comparison types are rejected with type names and a Rust-style source span | [`TestExecuteSQLQueryDiagnosesIncompatibleLiteralComparisonTypes`](sql_production_test.go) |
| Case-sensitive UTF-8 binary text collation is stable for comparison and ordering | [`TestExecuteSQLQueryUsesCaseSensitiveUTF8BinaryStringCollation`](sql_production_test.go) |
| Equality `LEFT`, `RIGHT`, and `FULL OUTER JOIN` use `HASH JOIN` while preserving unmatched sides; complex join predicates retain the general evaluator | [`TestExecuteSQLQuerySupportsRightAndFullOuterJoin`](sql_production_test.go), [`TestExecuteSQLQueryUsesHashJoinForEqualityOuterJoins`](sql_production_test.go) |
| `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`; malformed `UNION ALL` source spans for missing, repeated, punctuated, literal, and incomplete right queries | [`TestExecuteSQLQuerySupportsUnionAndUnionAll`](sql_production_test.go), [`TestExecuteSQLQueryUnionAllDiagnosticsPointAtTheOffendingToken`](sql_production_test.go) |
| Source-free `EXPLAIN` plan, including static hash/composite-index eligibility for equality joins; `EXPLAIN ANALYZE` total and per-operator row/timing statistics; malformed prefix source spans | [`TestExecuteSQLQueryExplainDescribesWithoutReadingSources`](sql_production_test.go), [`TestExecuteSQLQueryExplainIdentifiesEqualityJoinCandidates`](sql_production_test.go), [`TestExecuteSQLQueryExplainAnalyzeReturnsMeasuredExecutionStats`](sql_production_test.go), [`TestExecuteSQLQueryExplainAnalyzeReportsPerOperatorStats`](sql_production_test.go), [`TestExecuteSQLQueryExplainDiagnosticsPointAtTheOffendingToken`](sql_production_test.go) |
| Derived-table sources | [`TestExecuteSQLQuerySupportsDerivedTableSubqueries`](sql_production_test.go) |
| Duplicate clauses and malformed structure | [`TestExecuteSQLQueryProductionRejectsStructuralErrors`](sql_production_test.go) |
| One Hatrie key has one typed value slot; type prefixes are required | [`TestHatrieTypesShareOneLogicalKeyNamespace`](sql_production_test.go) |
| Context cancellation plus configurable join-work, result-byte, sort-memory, and group-memory budgets | [`TestExecuteSQLQueryContextEnforcesBudgetsAndCancellation`](sql_production_test.go) |
| SQL three-valued null semantics for comparisons, `AND`, and `OR` | [`TestExecuteSQLQueryUsesThreeValuedNullLogic`](sql_production_test.go) |
| Typed `$1` positional parameters in expressions and `CACHE` sources; missing/zero parameter diagnostics; HTTP JSON parameters | [`TestExecuteSQLQueryParametersBindTypedValuesAndDiagnosePositions`](sql_production_test.go), [`TestMonitoringSQLRouteExecutesReadOnlyQueryAndFormatsSyntaxErrors`](sql_http_test.go) |
| Bounded LRU prepared-query templates cache parsed parameterized SQL, bind fresh values for every execution, preserve `CACHE`, `VALUES`, and nested-query parameters without cross-request state, and retain recently reused templates | [`TestSQLPreparedQueryCacheReusesImmutableTemplateAndBindsFreshParameters`](sql_production_test.go), [`TestSQLPreparedQueryCacheBindsValuesAndNestedQueryParameters`](sql_production_test.go), [`TestSQLPreparedQueryCacheEvictsLeastRecentlyUsedTemplate`](sql_production_test.go) |
| Exact `DECIMAL` literals and casts plus explicit `CAST` to `TEXT`, `NUMBER`, `DECIMAL`, `BOOLEAN`, `DATE`, or `TIMESTAMP`, including source-spanned syntax and dynamic conversion failures in projections, predicates, joins, grouping, `HAVING`, ordering, and windows | [`TestExecuteSQLQueryPreservesExactDecimalValues`](sql_production_test.go), [`TestExecuteSQLQueryCastsTypedValuesAndDiagnosesDynamicFailures`](sql_production_test.go), [`TestSQLCastSyntaxDiagnosticsIdentifyTheFault`](sql_production_test.go) |
| Typed `CACHE` JSON field schemas (`TEXT`, `NUMBER`, `INTEGER`, `DECIMAL`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `JSON`), query-local conversion, and source-spanned row/field errors | [`TestHatTrieSQLTypedJSONFieldsValidateAndConvertRows`](sql_production_test.go), [`TestSQLAcceptedKeywordInventory`](sql_function_test.go) |
| Bounded stateless cursor pages and rejection of cursor reuse with a different query | [`TestMonitoringSQLRoutePaginatesWithBoundOpaqueCursor`](sql_http_test.go) |
| True row-by-row execution for stream-compatible `CACHE`/`VALUES` queries, including one held-and-released source snapshot, NDJSON HTTP records, streamability diagnostics, and the one-response generic Go `QueryRows` SDK | [`TestExecuteSQLQueryRowsStreamsCompatibleSourceRows`](sql_production_test.go), [`TestExecuteSQLQueryRowsHoldsAndReleasesSnapshot`](sql_production_test.go), [`TestMonitoringSQLRouteStreamsNDJSONRows`](sql_http_test.go), [`TestSQLConnQueryRowsDecodesAndStopsOnCallbackError`](sql_client_test.go), [`TestSQLConnQueryRowsConsumesOneNDJSONResponse`](sql_client_test.go) |
| Streamed chains of indexed equality INNER/LEFT CACHE joins probe each next right index per current row without materializing a source or result slice; expanded matches obey the global row budget | [`TestExecuteSQLQueryRowsStreamsIndexedInnerAndLeftJoin`](sql_production_test.go), [`TestExecuteSQLQueryRowsStreamsChainedIndexedJoins`](sql_production_test.go), [`TestExecuteSQLQueryRowsStreamsIndexedJoinsHonorMaxRows`](sql_production_test.go) |
| Repeated source references read one per-query snapshot | [`TestExecuteSQLQueryUsesOneSnapshotForRepeatedSources`](sql_production_test.go) |
| Optional JSON equality/range indexes; longest matching multi-field equality index; refreshed cardinality and posting-list-skew statistics; exact caller-supplied per-value estimates without enumerating values; selective `AND` predicates; and `INDEX SCAN`/`INDEX JOIN` planning (single-column inner plus null-preserving left/right, and composite-key inner/left). The suite also covers refresh after a cache write and authenticated `/api/sql` forwarding. | [`TestHatTrieOptionalSQLJSONFieldIndexRefreshesAndPlansIndexScan`](sql_production_test.go), [`TestHatTrieOptionalSQLJSONFieldIndexSupportsRangePredicates`](sql_production_test.go), [`TestHatTrieOptionalSQLJSONFieldIndexSelectsConjunctivePredicate`](sql_production_test.go), [`TestHatTrieSQLCompositeJSONIndexPlansAndReportsStatistics`](sql_production_test.go), [`TestHatTrieSQLJSONIndexStatsExposeSkewAndExplainEstimate`](sql_production_test.go), [`TestHatTrieSQLJSONIndexValueEstimateUsesExactPostingCount`](sql_production_test.go), [`TestHatTrieCompositeJSONIndexProbesMultiColumnInnerAndLeftJoin`](sql_production_test.go), [`TestHatTrieOptionalSQLJSONFieldIndexProbesInnerJoin`](sql_production_test.go), [`TestHatTrieOptionalSQLJSONFieldIndexProbesLeftJoin`](sql_production_test.go), [`TestHatTrieOptionalSQLJSONFieldIndexProbesRightJoin`](sql_production_test.go), [`TestHatTrieOptionalSQLJSONFieldIndexRightJoinRespectsWhere`](sql_production_test.go), [`TestHatTrieOptionalSQLJSONFieldIndexRightJoinDoesNotMatchNulls`](sql_production_test.go), [`TestHatTrieOptionalSQLJSONFieldIndexRightJoinHonorsExecutionLimits`](sql_production_test.go), [`TestMonitoringSQLRouteUsesHatTrieJSONFieldIndex`](sql_http_test.go), [`TestMonitoringSQLRouteUsesHatTrieCompositeJSONIndex`](sql_http_test.go) |
| Partitioned `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, and numeric `SUM`/`AVG`/`MIN`/`MAX` windows, including validated `ROWS BETWEEN … AND …` moving frames | [`TestExecuteSQLQuerySupportsPartitionedWindowFunctions`](sql_production_test.go), [`TestExecuteSQLQuerySupportsDenseRankLagAndLeadWindows`](sql_production_test.go), [`TestExecuteSQLQuerySupportsRowsWindowFramesAndMovingAggregates`](sql_production_test.go) |
| Direct self-recursive CTE seed/working-table evaluation (`UNION ALL` hierarchy traversal), plus clear diagnostics when `RECURSIVE` or its required set term is absent | [`TestExecuteSQLQuerySupportsRecursiveCTEHierarchy`](sql_production_test.go), [`TestSQLKeywordInventoryReportsContextualDiagnostics`](sql_function_test.go) |
| Configured recursive CTE expansion-depth guard | [`TestExecuteSQLQueryEnforcesRecursiveCTEDepthLimit`](sql_production_test.go) |
| Opt-in recursive `UNION ALL` cycle detector | [`TestExecuteSQLQueryDetectsRecursiveCTECycles`](sql_production_test.go) |
| Recursive `SEARCH BREADTH FIRST` ordering and SQL-visible `CYCLE` marker columns, including terminating a repeated frontier | [`TestExecuteSQLQueryRecursiveCTESearchAndCycleColumns`](sql_production_test.go) |
| Every accepted relational keyword/contextual word: clauses, `EXPLAIN`/`ANALYZE`, all join forms, boolean/null operators, source forms, sort/pagination, set operations, and literals | Named positive subtests in [`TestSQLAcceptedKeywordInventory`](sql_function_test.go) |
| Contextual clause diagnostics (`INNER` requires `JOIN`, `GROUP`/`ORDER` require `BY`, `IS` requires `NULL`, unsupported `INTERSECT ALL`) | [`TestSQLKeywordInventoryReportsContextualDiagnostics`](sql_function_test.go) |

## UDFs, API, and robustness

| Requirement | Evidence |
| --- | --- |
| GO parsing, typed arguments, source spans, arithmetic and divide-by-zero diagnostics | [`sql_function_test.go`](sql_function_test.go) |
| Every `CREATE FUNCTION` syntax word, language (`GO`, `LUA`, `WASM`, `JS`), and declared type (`ANY`, `INTEGER`, `NUMBER`, `TEXT`, `BOOLEAN`) | Named subtests in [`TestSQLAcceptedKeywordInventory`](sql_function_test.go) |
| GO UDF use in `WHERE` and `SELECT` | [`TestExecuteSQLQueryUsesGoFunctionInWhereAndSelect`](sql_function_test.go) |
| Optional sandboxed LuaJIT vector batching and conversion rejection | [`sql_lua_luajit_test.go`](sql_lua_luajit_test.go) (`-tags luajit`) |
| Numeric Wazero Wasm ABI, type rejection, and runtime close | [`sql_wazero_test.go`](sql_wazero_test.go) |
| Sandboxed JavaScript→Javy→Wazero vector batching, source spans, and timeout interruption | [`sql_javascript_javy_test.go`](sql_javascript_javy_test.go) (`-tags javy`, explicit compiler path) |
| HTTP SQL/function malformed requests, normal calls, and `EXPLAIN ANALYZE` JSON | [`TestMonitoringSQLRouteExecutesReadOnlyQueryAndFormatsSyntaxErrors`](sql_http_test.go), [`TestMonitoringSQLFunctionRouteRegistersTypedGoFunction`](sql_http_test.go), [`TestMonitoringSQLRoutesRejectMalformedRequests`](sql_production_test.go) |
| CLI command, relational query, and function routes | [`cmd/hatrie-cli/sql_test.go`](cmd/hatrie-cli/sql_test.go) |
| Generic Go SDK decode and callback early stop | [`sql_client_test.go`](sql_client_test.go) |
| Parser and executor panic resistance | [`FuzzSQLParsersDoNotPanic`](sql_production_test.go), [`FuzzExecuteSQLQueryDoesNotPanic`](sql_production_test.go) |
| Deterministic generated-reference cases plus SQLite differential cases for INNER/LEFT/RIGHT/FULL joins (including unmatched-side NULLs), grouping, windows, timestamps, recursive CTEs, and indexed filters; the index case also proves `INDEX SCAN` rather than only matching output | [`TestSQLGeneratedReferenceCasesForJoinsGroupsAndSets`](sql_production_test.go), [`TestSQLDifferentialAgainstSQLiteForJoinsGroupsAndWindows`](sql_production_test.go) |
| Full suite, race, vet, and package coverage | `make test` (`scripts/verify-go.sh`) |

## Deliberately unsupported

The parser must reject or diagnose these rather than pretend to implement them:

- Correlated subqueries and mutually-recursive CTEs.
- `INTERSECT ALL` and `EXCEPT ALL`.
- SQL writes through relational `SELECT`; cache mutations use the separately
  compiled command SQL forms.
- General text/JSON Wasm ABI. JavaScript is supported through the separately
  documented Javy-to-Wazero batch ABI; see [UDF.md](UDF.md).

The repository's canonical full verification entry point is `make test`.
