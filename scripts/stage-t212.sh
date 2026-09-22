#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  README.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  T212_WAL_RETENTION_REPLICA_ACKS.md \
  hat/hatJournal/journal.go \
  hat/hatCache/journal.go \
  hat/hatCache/journal_segments.go \
  hat/hatCache/journal_replica_retention.go \
  hat/hatCache/t212_replica_retention_test.go \
  hat/hatCache/t212_replica_retention_baseline_benchmark_test.go \
  hat/hatCache/t212_replica_retention_benchmark_test.go \
  scripts/test-t212.sh \
  scripts/test-t212-package.sh \
  scripts/benchmark-t212-baseline.sh \
  scripts/benchmark-t212.sh \
  scripts/format-t212.sh \
  scripts/race-t212.sh \
  scripts/vet-t212.sh \
  scripts/verify-docs-t212.sh \
  scripts/stage-t212.sh \
  scripts/commit-t212.sh \
  scripts/push-t212.sh
git diff --cached --check
git diff --cached --name-status
