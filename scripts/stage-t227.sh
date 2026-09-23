#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  SCHEMA_CONSTRAINTS.md \
  T227_FIELD_VALIDATION.md \
  hat/hatSchema/constraint.go \
  hat/hatSchema/field_validation.go \
  hat/hatSchema/t227_field_validation_test.go \
  scripts/benchmark-t227.sh \
  scripts/commit-t227.sh \
  scripts/format-t227.sh \
  scripts/push-t227.sh \
  scripts/race-t227.sh \
  scripts/review-t227.sh \
  scripts/stage-t227.sh \
  scripts/test-t227.sh \
  scripts/verify-t227.sh \
  scripts/vet-t227.sh
