#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  Makefile \
  MZ043_OPERATOR_FRONTIER_LAG.md \
  README.md \
  cmd/hatrie-cli/main.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/mz043_operator_frontier_monitoring_benchmark_test.go \
  hat/hatCache/mz043_operator_frontier_monitoring_test.go \
  hat/hatCache/operator_frontier_monitoring.go \
  hat/hatMetrics/mz043_operator_frontier_benchmark_test.go \
  hat/hatMetrics/mz043_operator_frontier_test.go \
  hat/hatMetrics/operator_frontier.go \
  scripts/benchmark-mz043-operator-frontier.sh \
  scripts/commit-mz043-operator-frontier.sh \
  scripts/format-mz043-operator-frontier.sh \
  scripts/push-mz043-operator-frontier.sh \
  scripts/race-mz043-operator-frontier.sh \
  scripts/review-mz043-operator-frontier.sh \
  scripts/test-mz043-operator-frontier.sh \
  scripts/test-mz043-restore-regression.sh \
  scripts/vet-mz043-operator-frontier.sh
git commit -m "feat: add operator frontier lag metrics"
