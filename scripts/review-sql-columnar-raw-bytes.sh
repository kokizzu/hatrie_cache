#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatCache/sql_query.go hat/hatCache/sql_columnar_raw_bytes_test.go hat/hatCache/sql_columnar_raw_bytes_benchmark_test.go scripts/inspect-sql-source-ownership.sh scripts/format-sql-columnar-raw-bytes.sh scripts/test-sql-columnar-raw-bytes.sh scripts/benchmark-sql-columnar-raw-bytes.sh scripts/review-sql-columnar-raw-bytes.sh scripts/commit-sql-columnar-raw-bytes.sh
git diff -- Makefile
go test ./hat/hatCache -run '^TestSQLColumnarRawBytesBatchUsesLockedRawStorage$' -count=1
go test -race ./hat/hatCache -run '^TestSQLColumnarRawBytesBatchUsesLockedRawStorage$' -count=1
go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarRawBytesSource$' -benchmem -count=5
go test ./...
