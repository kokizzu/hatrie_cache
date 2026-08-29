#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/query.go hat/hatCache/sql_columnar_numeric_conjunction_test.go hat/hatCache/sql_columnar_numeric_conjunction_benchmark_test.go scripts/inspect-sql-execution-arena.sh scripts/format-sql-columnar-numeric-conjunction.sh scripts/test-sql-columnar-numeric-conjunction.sh scripts/benchmark-sql-columnar-numeric-conjunction.sh scripts/review-sql-columnar-numeric-conjunction.sh scripts/commit-sql-columnar-numeric-conjunction.sh
git diff -- Makefile
go test ./hat/hatCache -run '^TestSQLColumnarScanUsesNumericVectorConjunction$' -count=1
go test -race ./hat/hatCache -run '^TestSQLColumnarScanUsesNumericVectorConjunction$' -count=1
go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarNumericConjunction$' -benchmem -count=5
go test ./...
