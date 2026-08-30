#!/bin/sh
set -eu

files='
BENCHMARK.md
Makefile
README.md
hat/hatSql/query.go
scripts/deliver-sql-query-rows-observation-bytes.sh
scripts/inspect-sql-expression-batch.sh
scripts/profile-sql-query-rows-stream.sh
'

git diff --check -- $files
git add -- $files
git diff --cached --check
git commit -m 'perf(sql): skip unobserved query rows bytes'
git push
