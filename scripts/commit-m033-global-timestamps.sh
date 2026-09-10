#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  GLOBAL_TIMESTAMP_ORACLE.md \
  INSPIRATION.md \
  Makefile \
  README.md \
  hat/hatReplication/global_timestamp_oracle.go \
  hat/hatReplication/global_timestamp_oracle_benchmark_test.go \
  hat/hatReplication/global_timestamp_oracle_test.go \
  scripts/benchmark-m033-global-timestamps.sh \
  scripts/commit-m033-global-timestamps.sh \
  scripts/format-m033-global-timestamps.sh \
  scripts/push-m033-global-timestamps.sh \
  scripts/review-m033-global-timestamps.sh \
  scripts/test-m033-global-timestamps.sh \
  scripts/test-race-m033-global-timestamps.sh
git diff --cached --check
git commit -m "feat(replication): add consensus-bound global timestamps"
