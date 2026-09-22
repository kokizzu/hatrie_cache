#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  INSPIRATION_ROUND2.md \
  BENCHMARK.md \
  T219_PACKED_HASH_INDEX.md \
  hat/hatDataStructure/packed_hash_index.go \
  hat/hatDataStructure/t219_packed_hash_index_test.go \
  hat/hatDataStructure/t219_packed_hash_index_benchmark_test.go \
  scripts/test-t219.sh \
  scripts/format-t219.sh \
  scripts/benchmark-t219.sh \
  scripts/race-t219.sh \
  scripts/vet-t219.sh \
  scripts/verify-docs-t219.sh \
  scripts/stage-t219.sh \
  scripts/commit-t219.sh \
  scripts/push-t219.sh
git diff --cached --check
git diff --cached --name-status
