#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git diff --stat -- \
  Makefile \
  README.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  MZ029_SPILLABLE_ARRANGEMENT.md \
  MZ029_PERSISTED_INDEX.md \
  hat/hatDataStructure/spillable_arrangement.go \
  hat/hatDataStructure/spillable_arrangement_index.go \
  hat/hatDataStructure/spillable_arrangement_index_buffered.go \
  hat/hatDataStructure/mz029_persisted_index_test.go \
  hat/hatDataStructure/mz029_persisted_index_internal_test.go \
  hat/hatDataStructure/mz029_persisted_index_fallback_test.go \
  scripts/test-mz029-persisted-index.sh \
  scripts/benchmark-mz029-persisted-index.sh \
  scripts/format-mz029-persisted-index.sh \
  scripts/review-mz029-persisted-index.sh
