#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  BENCHMARK.md \
  C230_MEMORY_OVERCOMMIT.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  hat/hatSql/c230_memory_overcommit_benchmark_test.go \
  hat/hatSql/c230_memory_overcommit_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-c230-memory-overcommit.sh \
  scripts/format-c230-memory-overcommit.sh \
  scripts/race-c230-memory-overcommit.sh \
  scripts/test-c230-memory-overcommit.sh \
  scripts/vet-c230-memory-overcommit.sh \
  scripts/stage-c230-memory-overcommit.sh \
  scripts/commit-c230-memory-overcommit.sh \
  scripts/push-c230-memory-overcommit.sh
git diff --cached --check
git status --short
