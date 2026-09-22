#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  README.md \
  T213_SCHEDULED_SNAPSHOTS.md \
  hat/hatCache/t213_scheduled_snapshot.go \
  hat/hatCache/t213_scheduled_snapshot_test.go \
  hat/hatCache/t213_scheduled_snapshot_benchmark_test.go \
  hat/hatCache/t213_scheduled_snapshot_baseline_benchmark_test.go \
  scripts/test-t213.sh \
  scripts/test-t213-package.sh \
  scripts/benchmark-t213-baseline.sh \
  scripts/benchmark-t213.sh \
  scripts/format-t213.sh \
  scripts/race-t213.sh \
  scripts/vet-t213.sh \
  scripts/verify-docs-t213.sh \
  scripts/stage-t213.sh \
  scripts/commit-t213.sh \
  scripts/push-t213.sh

git diff --cached --check
git diff --cached --stat
