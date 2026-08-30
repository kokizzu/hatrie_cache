#!/usr/bin/env sh
set -eu

git add -- \
  BENCHMARK.md \
  Makefile \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_typed_index_test.go \
  scripts/deliver-sql-typed-index-order.sh
git diff --cached --check
git commit -m 'perf(sql): use typed int64 index for ordering'
git push origin master
