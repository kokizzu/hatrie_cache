#!/bin/sh
set -eu

files='
BENCHMARK.md
Makefile
README.md
hat/hatSql/expression_batch_leaf_test.go
hat/hatSql/query.go
scripts/deliver-sql-direct-batch-leaves.sh
scripts/inspect-sql-expression-batch.sh
scripts/test-sql-expression-batch.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): batch field and literal expressions'
git push
