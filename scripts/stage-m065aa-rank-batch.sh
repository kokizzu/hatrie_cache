#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INCREMENTAL_RANK_WINDOW.md \
  INSPIRATION.md \
  Makefile \
  hat/hatSql/m065_mutable_rank_window_test.go \
  hat/hatSql/m065_rank_window_benchmark_test.go \
  hat/hatSql/mutable_rank_window.go \
  scripts/commit-m065aa-rank-batch.sh \
  scripts/push-m065aa-rank-batch.sh \
  scripts/review-m065aa-rank-batch.sh \
  scripts/stage-m065aa-rank-batch.sh
printf '%s\n' 'Staged M065aa batched rank fast-path paths.'
