#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  COMPACTION_SCHEDULER_STATS.md \
  hat/hatStorage/compaction_scheduler.go \
  hat/hatStorage/compaction_scheduler_stats.go \
  hat/hatStorage/compaction_scheduler_stats_benchmark_test.go \
  hat/hatStorage/compaction_scheduler_stats_test.go \
  scripts/benchmark-tt036-baseline.sh \
  scripts/benchmark-tt036-scheduler-stats.sh \
  scripts/format-tt036-scheduler-stats.sh \
  scripts/test-tt036-scheduler-stats.sh \
  scripts/test-race-tt036-scheduler-stats.sh \
  scripts/vet-tt036-scheduler-stats.sh \
  scripts/verify-tt036-docs.sh \
  scripts/review-tt036-scheduler-stats.sh \
  scripts/commit-tt036-scheduler-stats.sh \
  scripts/push-tt036-scheduler-stats.sh
git commit -m 'hatStorage: expose compaction scheduler stats'
