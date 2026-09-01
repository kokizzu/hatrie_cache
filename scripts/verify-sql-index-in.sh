#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_index_in_test.go hat/hatCache/sql_index_in_benchmark_test.go hat/hatSql/query.go
go test ./hat/hatCache -run '^TestSQLJSONIndexLiteralIN|^TestSQLJSONIndexUsesIndexForLiteralIN$' -count=1
go test -race ./hat/hatCache -run '^TestSQLJSONIndexLiteralIN|^TestSQLJSONIndexUsesIndexForLiteralIN$' -count=1
git diff --check -- hat/hatCache/sql_index_in_test.go hat/hatCache/sql_index_in_benchmark_test.go hat/hatSql/query.go ADOPTED_QUERY_ENGINE_IDEAS.md Makefile scripts/test-sql-index-in.sh scripts/test-race-sql-index-in.sh scripts/benchmark-sql-index-in.sh scripts/verify-sql-index-in.sh
