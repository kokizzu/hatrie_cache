#!/usr/bin/env sh
set -eu

git add -- \
  Makefile \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_typed_index_test.go \
  scripts/deliver-sql-typed-index-value-estimate.sh
git diff --cached --check
git commit -m 'perf(sql): estimate typed index equality exactly'
git push origin master
