#!/usr/bin/env bash
set -euo pipefail

git diff --check
git status --short
git add BENCHMARK.md INSPIRATION_ROUND2.md README.md T201_PER_SPACE_WRITE_QUORUM.md Makefile \
  hat/hatCache/grpc.go \
  hat/hatCache/local_partition.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/t201_per_space_write_quorum_benchmark_test.go \
  hat/hatCache/t201_per_space_write_quorum_test.go \
  hat/hatCache/write_quorum_policy.go \
  scripts/benchmark-t201.sh \
  scripts/format-t201.sh \
  scripts/race-t201.sh \
  scripts/test-t201-package.sh \
  scripts/test-t201-per-space-write-quorum.sh \
  scripts/vet-t201.sh \
  scripts/stage-t201.sh \
  scripts/commit-t201.sh \
  scripts/push-t201.sh
git diff --cached --check
git diff --cached --stat
