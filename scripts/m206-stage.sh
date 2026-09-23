#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  M206_UPSERT_ENVELOPES.md \
  Makefile \
  README.md \
  hat/hatSql/debezium_changefeed.go \
  hat/hatSql/m206_upsert_envelope_baseline_benchmark_test.go \
  hat/hatSql/m206_upsert_envelope_benchmark_test.go \
  hat/hatSql/m206_upsert_envelope_test.go \
  hat/hatSql/upsert_envelope.go \
  scripts/m206-benchmark-baseline.sh \
  scripts/m206-benchmark.sh \
  scripts/m206-format.sh \
  scripts/m206-race.sh \
  scripts/m206-test-package.sh \
  scripts/m206-test.sh \
  scripts/m206-vet.sh \
  scripts/m206-stage.sh \
  scripts/m206-commit.sh \
  scripts/m206-push.sh
