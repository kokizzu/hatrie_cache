#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  T212_WAL_REPLICA_RETENTION.md \
  hat/hatCache/journal.go \
  hat/hatCache/journal_replica_retention.go \
  hat/hatCache/journal_segments.go \
  hat/hatCache/t212_replica_retention_baseline_benchmark_test.go \
  hat/hatCache/t212_replica_retention_benchmark_test.go \
  hat/hatCache/t212_replica_retention_test.go \
  hat/hatJournal/journal.go \
  scripts/benchmark-t212-before.sh \
  scripts/benchmark-t212.sh \
  scripts/commit-t212.sh \
  scripts/format-t212.sh \
  scripts/push-t212.sh \
  scripts/race-t212.sh \
  scripts/stage-t212.sh \
  scripts/test-t212-package.sh \
  scripts/test-t212.sh \
  scripts/verify-t212-scope.sh \
  scripts/vet-t212.sh
