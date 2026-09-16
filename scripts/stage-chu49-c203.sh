#!/bin/sh
set -eu

git diff --check
git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CHU49_SKIP_INDEX_EXPLAIN.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatCache/ch_u49_skip_index_benchmark_test.go \
  hat/hatCache/ch_u49_skip_index_explain_test.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/sql_index_diagnostics.go \
  hat/hatCache/sql_json_path_skip.go \
  hat/hatCache/sql_query.go \
  hat/hatSql/catalog.go \
  hat/hatSql/contracts.go \
  hat/hatSql/index_diagnostics.go \
  hat/hatSql/model.go \
  hat/hatSql/query.go \
  scripts/benchmark-chu49-c203.sh \
  scripts/format-chu49-c203.sh \
  scripts/race-chu49-c203.sh \
  scripts/stage-chu49-c203.sh \
  scripts/commit-chu49-c203.sh \
  scripts/push-chu49-c203.sh \
  scripts/test-chu49-c203.sh \
  scripts/vet-chu49-c203.sh
git diff --cached --check
git diff --cached --stat
