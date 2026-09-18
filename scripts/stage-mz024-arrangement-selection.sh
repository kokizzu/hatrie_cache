#!/usr/bin/env bash
set -euo pipefail

git add Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  MZ024_ARRANGEMENT_KEY_SELECTION.md \
  README.md \
  hat/hatSql/mu012_arrangement_explain.go \
  hat/hatSql/mz024_arrangement_selection.go \
  hat/hatSql/mz024_arrangement_selection_public_test.go \
  hat/hatSql/mz024_arrangement_selection_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-mz024-arrangement-selection.sh \
  scripts/commit-mz024-arrangement-selection.sh \
  scripts/format-mz024-arrangement-selection.sh \
  scripts/push-mz024-arrangement-selection.sh \
  scripts/race-mz024-arrangement-selection.sh \
  scripts/review-mz024.sh \
  scripts/stage-mz024-arrangement-selection.sh \
  scripts/test-mz024-arrangement-selection.sh \
  scripts/test-mz024-package.sh \
  scripts/vet-mz024-arrangement-selection.sh
