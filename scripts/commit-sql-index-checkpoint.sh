#!/bin/sh
set -eu

git add -- \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  SQL_INDEX_REBUILD_CHECKPOINT.md \
  hat/hatCache/main.go \
  hat/hatCache/sql_index_checkpoint.go \
  hat/hatCache/sql_index_checkpoint_benchmark_test.go \
  hat/hatCache/sql_index_checkpoint_test.go \
  hat/hatCache/sql_index_progress.go \
  hat/hatCache/sql_query.go \
  scripts/benchmark-sql-index-checkpoint.sh \
  scripts/commit-sql-index-checkpoint.sh \
  scripts/format-sql-index-checkpoint.sh \
  scripts/push-sql-index-checkpoint.sh \
  scripts/review-sql-index-checkpoint.sh \
  scripts/test-race-sql-index-checkpoint.sh \
  scripts/test-sql-index-checkpoint.sh \
  scripts/vet-sql-index-checkpoint.sh
git diff --cached --check
git commit -m "feat: add durable SQL index rebuild checkpoints"
