#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU38_CONFLICT_INTROSPECTION.md \
  hat/hatReplication/conflict_introspection.go \
  hat/hatReplication/conflict_introspection_benchmark_test.go \
  hat/hatReplication/conflict_introspection_test.go \
  scripts/benchmark-round40-conflict.sh \
  scripts/race-round40-conflict-package.sh \
  scripts/race-round40-conflict.sh \
  scripts/commit-round40-conflict.sh \
  scripts/push-round40-conflict.sh \
  scripts/stage-round40-conflict.sh \
  scripts/test-round40-conflict-package.sh \
  scripts/test-round40-conflict.sh \
  scripts/verify-round40-conflict-docs.sh \
  scripts/vet-round40-conflict.sh
git diff --cached --check
git status --short
