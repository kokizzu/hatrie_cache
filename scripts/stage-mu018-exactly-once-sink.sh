#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  Makefile \
  MU018_EXACTLY_ONCE_SINK.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  SQL_SINK_PROGRESS.md \
  hat/hatSql/mu018_exactly_once_sink_baseline_benchmark_test.go \
  hat/hatSql/mu018_exactly_once_sink_benchmark_test.go \
  hat/hatSql/mu018_exactly_once_sink_test.go \
  hat/hatSql/sql_sink_commit.go \
  hat/hatSql/sql_sink_exactly_once.go \
  hat/hatSql/sql_sink_exactly_once_codec.go \
  scripts/benchmark-mu018-baseline.sh \
  scripts/benchmark-mu018-durable-sink.sh \
  scripts/benchmark-mu018-exactly-once-sink.sh \
  scripts/format-mu018-exactly-once-sink.sh \
  scripts/stage-mu018-exactly-once-sink.sh \
  scripts/commit-mu018-exactly-once-sink.sh \
  scripts/push-mu018-exactly-once-sink.sh \
  scripts/race-mu018-exactly-once-sink.sh \
  scripts/review-mu018-exactly-once-sink.sh \
  scripts/test-mu018-exactly-once-sink.sh \
  scripts/test-mu018-package.sh \
  scripts/vet-mu018-exactly-once-sink.sh
