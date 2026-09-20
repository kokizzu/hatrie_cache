#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
  Makefile \
  hat/hatSql/m065v_mutable_offset_window.go \
  hat/hatSql/m065v_mutable_offset_window_test.go \
  hat/hatSql/m065v_mutable_offset_window_baseline_benchmark_test.go \
  scripts/benchmark-m065v-mutable-offset-window.sh \
  scripts/format-m065v-mutable-offset-window.sh \
  scripts/test-m065v-mutable-offset-window.sh \
  scripts/verify-m065v-mutable-offset-window.sh \
  INCREMENTAL_MUTABLE_OFFSET_WINDOW.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INSPIRATION.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md
