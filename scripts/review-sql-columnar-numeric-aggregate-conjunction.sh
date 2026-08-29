#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/query.go hat/hatCache/sql_columnar_numeric_aggregate_conjunction_test.go hat/hatCache/sql_columnar_numeric_aggregate_conjunction_benchmark_test.go scripts/format-sql-columnar-numeric-aggregate-conjunction.sh scripts/test-sql-columnar-numeric-aggregate-conjunction.sh scripts/benchmark-sql-columnar-numeric-aggregate-conjunction.sh scripts/review-sql-columnar-numeric-aggregate-conjunction.sh scripts/commit-sql-columnar-numeric-aggregate-conjunction.sh
git diff -- Makefile
go test ./hat/hatCache -run '^TestSQLColumnarNumericAggregateUsesNumericVectorConjunction$' -count=1
go test -race ./hat/hatCache -run '^TestSQLColumnarNumericAggregateUsesNumericVectorConjunction$' -count=1
go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarNumericAggregateConjunction$' -benchmem -count=5
go test ./...
