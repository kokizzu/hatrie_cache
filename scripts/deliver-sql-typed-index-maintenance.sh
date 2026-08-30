#!/usr/bin/env sh
set -eu

git add -- \
  Makefile \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_typed_index_test.go \
  scripts/deliver-sql-typed-index-maintenance.sh \
  scripts/test-sql-typed-index.sh
git diff --cached --check
git commit -m 'perf(sql): schedule typed index maintenance'
git push origin master
