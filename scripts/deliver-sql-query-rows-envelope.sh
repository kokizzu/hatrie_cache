#!/bin/sh
set -eu

files='
BENCHMARK.md
Makefile
README.md
hat/hatSql/expression_batch_leaf_test.go
hat/hatSql/query.go
hat/hatSql/stream_query_rows_benchmark_test.go
scripts/benchmark-sql-query-rows-stream.sh
scripts/deliver-sql-query-rows-envelope.sh
scripts/inspect-sql-expression-batch.sh
scripts/test-sql-expression-batch.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): compact query rows source envelopes'
git push
