#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C238_MUTATION_PROGRESS.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatSql/mutation_controller.go \
  hat/hatSql/c238_mutation_progress_benchmark_test.go \
  hat/hatSql/c238_mutation_progress_test.go \
  scripts/benchmark-c238.sh \
  scripts/commit-c238.sh \
  scripts/format-c238.sh \
  scripts/push-c238.sh \
  scripts/race-c238.sh \
  scripts/test-c238-package.sh \
  scripts/test-c238-related.sh \
  scripts/test-c238.sh \
  scripts/verify-c238-docs.sh \
  scripts/vet-c238.sh
git diff --cached --check
git commit -m "feat(sql): expose mutation progress estimates"
