#!/usr/bin/env bash
set -euo pipefail

git add -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C249_OFFSET_INSPECTION.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatReplication/c249_offset_inspection.go \
  hat/hatReplication/c249_offset_inspection_benchmark_test.go \
  hat/hatReplication/c249_offset_inspection_test.go \
  scripts/benchmark-c249-offset-inspection.sh \
  scripts/format-c249-offset-inspection.sh \
  scripts/inspect-c249-scope.sh \
  scripts/race-c249-offset-inspection.sh \
  scripts/review-c249-offset-inspection.sh \
  scripts/stage-c249-offset-inspection.sh \
  scripts/test-c249-offset-inspection-package.sh \
  scripts/test-c249-offset-inspection.sh \
  scripts/vet-c249-offset-inspection.sh

git diff --cached --check
git diff --cached --stat
