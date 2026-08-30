#!/usr/bin/env sh
set -eu

git add -- \
  Makefile \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_typed_index_test.go \
  scripts/deliver-sql-typed-index-range-stats.sh
git diff --cached --check
git commit -m 'perf(sql): report typed index range histograms'
git push origin master
