#!/usr/bin/env sh
set -eu

git add -- \
  BENCHMARK.md \
  INDEX_PROPOSAL.md \
  Makefile \
  hat/hatCache/main.go \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_typed_index_benchmark_test.go \
  hat/hatCache/sql_typed_index_test.go \
  scripts/bench-sql-typed-index.sh \
  scripts/deliver-sql-typed-index.sh \
  scripts/format-sql-typed-index.sh \
  scripts/test-sql-typed-index.sh
git diff --cached --check
git commit -m 'perf(sql): add opt-in typed int64 index'
git push origin master
