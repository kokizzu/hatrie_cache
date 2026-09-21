#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M208_DIFFERENTIAL_MULTIPLICITY_FOLDING.md \
  Makefile \
  hat/hatSql/debezium_changefeed.go \
  hat/hatSql/m206_upsert_envelope.go \
  hat/hatSql/m208_differential_folding.go \
  hat/hatSql/m208_differential_folding_baseline_benchmark_test.go \
  hat/hatSql/m208_differential_folding_test.go \
  scripts/benchmark-m208-differential-folding-baseline.sh \
  scripts/benchmark-m208-differential-folding.sh \
  scripts/commit-m208-differential-folding.sh \
  scripts/format-m208-differential-folding.sh \
  scripts/push-m208-differential-folding.sh \
  scripts/race-m208-differential-folding.sh \
  scripts/test-m208-changefeeds.sh \
  scripts/test-m208-differential-folding.sh \
  scripts/vet-m208-differential-folding.sh \
  scripts/stage-m208-differential-folding.sh
