#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --stat -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C245_VERTICAL_TTL_DELETE.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatDataStructure/vertical_ttl_delete.go \
  hat/hatDataStructure/vertical_ttl_delete_benchmark_test.go \
  hat/hatDataStructure/vertical_ttl_delete_test.go \
  scripts/benchmark-c245-vertical-ttl-delete.sh \
  scripts/format-c245-vertical-ttl-delete.sh \
  scripts/inspect-c245-scope.sh \
  scripts/review-c245-vertical-ttl-delete.sh \
  scripts/race-c245-vertical-ttl-delete.sh \
  scripts/stage-c245-vertical-ttl-delete.sh \
  scripts/test-c245-vertical-ttl-delete.sh \
  scripts/vet-c245-vertical-ttl-delete.sh \
  scripts/commit-c245-vertical-ttl-delete.sh \
  scripts/push-c245-vertical-ttl-delete.sh
git status --short -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C245_VERTICAL_TTL_DELETE.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatDataStructure/vertical_ttl_delete.go \
  hat/hatDataStructure/vertical_ttl_delete_benchmark_test.go \
  hat/hatDataStructure/vertical_ttl_delete_test.go \
  scripts/benchmark-c245-vertical-ttl-delete.sh \
  scripts/format-c245-vertical-ttl-delete.sh \
  scripts/inspect-c245-scope.sh \
  scripts/review-c245-vertical-ttl-delete.sh \
  scripts/race-c245-vertical-ttl-delete.sh \
  scripts/stage-c245-vertical-ttl-delete.sh \
  scripts/test-c245-vertical-ttl-delete.sh \
  scripts/vet-c245-vertical-ttl-delete.sh \
  scripts/commit-c245-vertical-ttl-delete.sh \
  scripts/push-c245-vertical-ttl-delete.sh
