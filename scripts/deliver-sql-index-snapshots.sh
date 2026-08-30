#!/usr/bin/env sh
set -eu

git add -- \
  BENCHMARK.md \
  Makefile \
  README.md \
  hat/hatCache/main.go \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_index_snapshot_benchmark_test.go \
  hat/hatCache/sql_index_snapshot_test.go \
  scripts/bench-sql-index-snapshots.sh \
  scripts/format-sql-index-snapshots.sh \
  scripts/test-sql-index-snapshots.sh \
  scripts/deliver-sql-index-snapshots.sh
git diff --cached --check
git commit -m 'perf(sql): share decoded source snapshots across indexes'
git push origin master
