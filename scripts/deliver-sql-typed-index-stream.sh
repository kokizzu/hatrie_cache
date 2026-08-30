#!/usr/bin/env sh
set -eu

git add -- \
  Makefile \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_typed_index_test.go \
  scripts/deliver-sql-typed-index-stream.sh
git diff --cached --check
git commit -m 'perf(sql): stream typed int64 ordered scans'
git push origin master
