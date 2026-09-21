#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile ENGINE_IDEAS.md BENCHMARK.md TT028_UPSERT_CONFLICT_HANDLERS.md \
  hat/hatSchema/materialized.go \
  hat/hatSchema/tt028_upsert_conflict_test.go \
  hat/hatSchema/tt028_upsert_conflict_benchmark_test.go \
  scripts/format-tt028-upsert.sh scripts/test-tt028-upsert.sh \
  scripts/test-tt028-package.sh scripts/race-tt028-upsert.sh \
  scripts/vet-tt028-upsert.sh scripts/benchmark-tt028-before.sh \
  scripts/benchmark-tt028-upsert.sh scripts/review-tt028-upsert.sh \
  scripts/stage-tt028-upsert.sh scripts/commit-tt028-upsert.sh \
  scripts/push-tt028-upsert.sh
