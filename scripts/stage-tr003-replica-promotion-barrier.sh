#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  README.md \
  TR003_REPLICA_PROMOTION_BARRIER.md \
  Makefile \
  hat/hatReplication/tr003_replica_promotion_barrier.go \
  hat/hatReplication/tr003_replica_promotion_barrier_benchmark_test.go \
  hat/hatReplication/tr003_replica_promotion_barrier_test.go \
  scripts/benchmark-tr003-baseline.sh \
  scripts/benchmark-tr003-replica-promotion-barrier.sh \
  scripts/commit-tr003-replica-promotion-barrier.sh \
  scripts/format-tr003-replica-promotion-barrier.sh \
  scripts/push-tr003-replica-promotion-barrier.sh \
  scripts/race-tr003-replica-promotion-barrier.sh \
  scripts/review-tr003-replica-promotion-barrier.sh \
  scripts/stage-tr003-replica-promotion-barrier.sh \
  scripts/test-tr003-package.sh \
  scripts/test-tr003-replica-promotion-barrier.sh \
  scripts/verify-tr003-docs.sh \
  scripts/vet-tr003-replica-promotion-barrier.sh
git diff --cached --check
git status --short
