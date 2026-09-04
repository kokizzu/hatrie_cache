#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '--- T063 worktree paths ---'
git status --short
printf '%s\n' '--- T063 whitespace check ---'
git diff --check
printf '%s\n' '--- T063 feature diff ---'
git diff HEAD -- \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION.md \
  README.md \
  REPLICATION_OPERATIONS.md \
  hat/hatCache/monitoring.go \
  hat/hatCache/replication.go \
  hat/hatCache/replication_benchmark_test.go \
  hat/hatCache/replication_test.go \
  hat/hatReplication/model.go \
  scripts/benchmark-t063.sh \
  scripts/format-t063.sh \
  scripts/review-t063.sh \
  scripts/test-race-t063.sh \
  scripts/test-t063.sh \
  scripts/vet-t063.sh
printf '%s\n' '--- Makefile diff ---'
git diff -- Makefile
