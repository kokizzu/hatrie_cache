#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  Makefile \
  BENCHMARK.md \
  README.md \
  INSPIRATION_ROUND2.md \
  T210_MASTER_MASTER_CONFLICT_HOOKS.md \
  hat/hatReplication/conflict_hook.go \
  hat/hatReplication/conflict_policy.go \
  hat/hatReplication/t210_conflict_hooks_test.go \
  hat/hatReplication/t210_conflict_hooks_benchmark_test.go \
  scripts/test-t210.sh \
  scripts/test-t210-package.sh \
  scripts/benchmark-t210-before.sh \
  scripts/benchmark-t210.sh \
  scripts/format-t210.sh \
  scripts/race-t210.sh \
  scripts/stage-t210.sh \
  scripts/vet-t210.sh \
  scripts/commit-t210.sh \
  scripts/push-t210.sh \
  scripts/verify-t210-scope.sh

printf '%s\n' '== status (unrelated user files are preserved) =='
git status --short
