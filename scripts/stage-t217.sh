#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  T217_IN_MEMORY_COLUMNAR_SPACE.md \
  hat/hatDataStructure/columnar_space.go \
  hat/hatDataStructure/t217_columnar_space_test.go \
  hat/hatDataStructure/t217_columnar_space_benchmark_test.go \
  scripts/format-t217.sh \
  scripts/test-t217.sh \
  scripts/benchmark-t217.sh \
  scripts/race-t217.sh \
  scripts/vet-t217.sh \
  scripts/verify-docs-t217.sh \
  scripts/stage-t217.sh \
  scripts/commit-t217.sh \
  scripts/push-t217.sh

git diff --cached --name-status
git diff --cached --check
