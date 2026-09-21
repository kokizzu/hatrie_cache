#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M209_MONOTONE_LOGICAL_TIMESTAMP.md \
  Makefile \
  hat/hatDataStructure/m209_monotone_timestamp.go \
  hat/hatDataStructure/m209_monotone_timestamp_test.go \
  hat/hatDataStructure/m209_monotone_timestamp_benchmark_test.go \
  hat/hatDataStructure/m209_monotone_timestamp_baseline_benchmark_test.go \
  hat/hatReplication/changefeed_progress.go \
  hat/hatReplication/m209_monotone_frontier_test.go \
  hat/hatReplication/m209_monotone_frontier_benchmark_test.go \
  hat/hatReplication/m209_monotone_frontier_baseline_benchmark_test.go \
  hat/hatSql/sql_source_frontier.go \
  hat/hatSql/m209_monotone_frontier_test.go \
  hat/hatSql/m209_monotone_frontier_benchmark_test.go \
  scripts/benchmark-m209-changefeed-frontier.sh \
  scripts/benchmark-m209-monotone-frontier-baseline.sh \
  scripts/benchmark-m209-monotone-frontier.sh \
  scripts/commit-m209-monotone-frontier.sh \
  scripts/format-m209-monotone-frontier.sh \
  scripts/push-m209-monotone-frontier.sh \
  scripts/race-m209-monotone-frontier.sh \
  scripts/stage-m209-monotone-frontier.sh \
  scripts/test-m209-monotone-frontier.sh \
  scripts/vet-m209-monotone-frontier.sh
