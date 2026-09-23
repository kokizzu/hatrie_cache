#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  README.md \
  T213_SCHEDULED_SNAPSHOTS.md \
  hat/hatCache/scheduled_snapshot.go \
  hat/hatCache/snapshot_manifest.go \
  hat/hatCache/t213_scheduled_snapshot_baseline_benchmark_test.go \
  hat/hatCache/t213_scheduled_snapshot_benchmark_test.go \
  hat/hatCache/t213_scheduled_snapshot_test.go \
  scripts/benchmark-t213-before.sh \
  scripts/benchmark-t213.sh \
  scripts/commit-t213.sh \
  scripts/format-t213.sh \
  scripts/push-t213.sh \
  scripts/race-t213.sh \
  scripts/stage-t213.sh \
  scripts/test-t213-package.sh \
  scripts/test-t213.sh \
  scripts/verify-t213-scope.sh \
  scripts/vet-t213.sh
