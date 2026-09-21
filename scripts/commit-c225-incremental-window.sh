#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C225_INCREMENTAL_WINDOW.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatSql/ch225_incremental_window.go \
  hat/hatSql/ch225_incremental_window_benchmark_test.go \
  hat/hatSql/ch225_incremental_window_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-c225-baseline.sh \
  scripts/commit-c225-incremental-window.sh \
  scripts/format-c225-incremental-window.sh \
  scripts/push-c225-incremental-window.sh \
  scripts/race-c225-incremental-window.sh \
  scripts/race-c225-package.sh \
  scripts/report-inspiration-backlog.sh \
  scripts/test-c225-incremental-window.sh \
  scripts/test-c225-package.sh \
  scripts/test-c225-window-suite.sh \
  scripts/vet-c225-incremental-window.sh
git commit -m "perf: maintain incremental SQL window aggregates"
