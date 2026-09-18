#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ035_ARRANGEMENT_LOCALITY.md \
  README.md \
  hat/hatSql/mu012_arrangement_explain.go \
  hat/hatSql/mz024_arrangement_selection.go \
  hat/hatSql/mz035_arrangement_locality_benchmark_test.go \
  hat/hatSql/mz035_arrangement_locality_public_test.go \
  hat/hatSql/mz035_arrangement_locality_test.go \
  scripts/benchmark-mz035-arrangement-locality.sh \
  scripts/commit-mz035.sh \
  scripts/format-mz035-arrangement-locality.sh \
  scripts/race-mz035-arrangement-locality.sh \
  scripts/review-mz035.sh \
  scripts/stage-mz035.sh \
  scripts/test-mz035-arrangement-locality.sh \
  scripts/test-mz035-package.sh \
  scripts/push-mz035.sh \
  scripts/vet-mz035-arrangement-locality.sh

git diff --cached --check
git diff --cached --stat
git status --short
