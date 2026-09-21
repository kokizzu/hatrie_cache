#!/usr/bin/env bash
set -euo pipefail

git add -- \
    ADOPTED_QUERY_ENGINE_IDEAS.md \
    BENCHMARK.md \
    INSPIRATION_ROUND2.md \
    M206_UPSERT_ENVELOPES.md \
    Makefile \
    hat/hatSql/m206_upsert_envelope.go \
    hat/hatSql/m206_upsert_envelope_test.go \
    hat/hatSql/m206_upsert_envelope_baseline_benchmark_test.go \
    scripts/benchmark-m206-upsert-envelope.sh \
    scripts/benchmark-m206-upsert-envelope-baseline.sh \
    scripts/commit-m206-upsert-envelope.sh \
    scripts/format-m206-upsert-envelope.sh \
    scripts/push-m206-upsert-envelope.sh \
    scripts/race-m206-upsert-envelope.sh \
    scripts/stage-m206-upsert-envelope.sh \
    scripts/test-m206-upsert-envelope-package.sh \
    scripts/test-m206-upsert-envelope.sh \
    scripts/vet-m206-upsert-envelope.sh
