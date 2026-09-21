#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C230_MEMORY_OVERCOMMIT.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatSql/ch230_memory_overcommit.go \
  hat/hatSql/ch230_memory_overcommit_benchmark_test.go \
  hat/hatSql/ch230_memory_overcommit_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-c230-memory-overcommit.sh \
  scripts/commit-c230-memory-overcommit.sh \
  scripts/format-c230-memory-overcommit.sh \
  scripts/push-c230-memory-overcommit.sh \
  scripts/race-c230-memory-overcommit.sh \
  scripts/race-c230-package.sh \
  scripts/test-c230-memory-overcommit.sh \
  scripts/test-c230-package.sh \
  scripts/vet-c230-memory-overcommit.sh
git commit -m "feat: add SQL memory overcommit queue"
