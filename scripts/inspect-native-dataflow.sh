#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '===== M052p implementation ====='
sed -n '1,220p' hat/hatSql/m052p_auto_native_dataflow.go
printf '%s\n' '===== Existing native operator implementation ====='
sed -n '1,320p' hat/hatSql/m052c_native_dataflow.go
sed -n '1,180p' hat/hatSql/m052c_native_dataflow_test.go
sed -n '1,150p' hat/hatSql/m052c_native_dataflow_benchmark_test.go
printf '%s\n' '===== Commit file list ====='
sed -n '1,100p' scripts/commit-m052p-auto-native-dataflow.sh
printf '%s\n' '===== Query execution observation state ====='
sed -n '120,205p' hat/hatSql/query.go
sed -n '700,800p' hat/hatSql/query.go
rg -n -C 2 'operatorSteps|Observer|QueryObserver' hat/hatSql/query.go hat/hatSql/contracts.go
printf '%s\n' '===== M052p tests and benchmarks ====='
rg -n '^func (Test|Benchmark)CompiledSQLAutomaticNativeDataflow' hat/hatSql --glob '*_test.go'
printf '%s\n' '===== Resolver contracts that must retain precedence ====='
sed -n '50,145p' hat/hatSql/contracts.go
sed -n '1070,1375p' hat/hatSql/contracts.go
rg -n 'type (SQL|Partitioned|Columnar|Borrowed|Segmented|Sorted|Composite|Secondary|Covering|Ordered|Keyset|Lookup|Indexed|Range|Stream|Streaming).*SourceResolver' hat/hatSql --glob '*.go'
sed -n '1,90p' hat/hatCache/sql_columnar_like_test.go
sed -n '1,125p' hat/hatSql/source_borrowed_test.go
sed -n '1,220p' hat/hatSql/partitioned_source_test.go
printf '%s\n' '===== M052p documentation ====='
rg -n -C 1 'M052p|M052q|M052r|DisableNativeDataflow' BENCHMARK.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION.md SQL_AUTO_NATIVE_DATAFLOW.md SQL_AUTO_NATIVE_OPERATORS.md SQL_AUTO_NATIVE_ORDERED.md
sed -n '18965,19020p' BENCHMARK.md
printf '%s\n' '===== Unchecked backlog candidates ====='
rg -n '^\- \[ \] (C153 |C154 |M032 |M033 |M037 |M038 |M052 |M090 |T042 |T047 |T103 |T150)' INSPIRATION.md
