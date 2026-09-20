#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  TU038_CONFLICT_INTROSPECTION.md \
  hat/hatReplication/conflict_policy.go \
  hat/hatReplication/tu038_conflict_introspection.go \
  hat/hatReplication/tu038_conflict_introspection_benchmark_test.go \
  hat/hatReplication/tu038_conflict_introspection_test.go \
  scripts/benchmark-tu038-conflict-introspection.sh \
  scripts/commit-tu038-conflict-introspection.sh \
  scripts/format-tu038-conflict-introspection.sh \
  scripts/push-tu038-conflict-introspection.sh \
  scripts/race-tu038-conflict-introspection.sh \
  scripts/review-tu038-conflict-introspection.sh \
  scripts/stage-tu038-conflict-introspection.sh \
  scripts/test-tu038-conflict-introspection.sh \
  scripts/test-tu038-package.sh \
  scripts/vet-tu038-conflict-introspection.sh
git diff --cached --check
git diff --cached --stat
git status --short
