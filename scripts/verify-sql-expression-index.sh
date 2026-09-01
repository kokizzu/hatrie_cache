#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_lower_index.go hat/hatCache/sql_lower_index_test.go hat/hatCache/sql_lower_index_benchmark_test.go hat/hatSql/expression_index.go hat/hatSql/query.go
go test ./hat/hatCache -run '^TestSQLJSONLowerIndex' -count=1
go test -race ./hat/hatCache -run '^TestSQLJSONLowerIndex' -count=1
git diff --check -- hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_lower_index.go hat/hatCache/sql_lower_index_test.go hat/hatCache/sql_lower_index_benchmark_test.go hat/hatSql/expression_index.go hat/hatSql/query.go ADOPTED_QUERY_ENGINE_IDEAS.md Makefile scripts/test-sql-expression-index.sh scripts/test-race-sql-expression-index.sh scripts/benchmark-sql-expression-index.sh scripts/verify-sql-expression-index.sh
