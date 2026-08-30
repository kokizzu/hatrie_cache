#!/bin/sh
set -eu

files='
BENCHMARK.md
Makefile
README.md
hat/hatSql/expression_batch_leaf_test.go
hat/hatSql/query.go
scripts/deliver-sql-query-rows-scalar.sh
scripts/inspect-sql-expression-batch.sh
scripts/test-sql-expression-batch.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): evaluate simple query rows expressions directly'
git push
