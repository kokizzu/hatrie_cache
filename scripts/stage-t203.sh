#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  README.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  T203_LEADER_FENCING.md \
  hat/hatCache/leader_fencing.go \
  hat/hatCache/monitoring.go \
  hat/hatCache/grpc.go \
  hat/hatCache/grpc_scalar_batch.go \
  hat/hatCache/grpc_structured_batch.go \
  hat/hatCache/t203_leader_fencing_test.go \
  hat/hatCache/t203_leader_fencing_benchmark_test.go \
  scripts/format-t203.sh \
  scripts/test-t203-leader-fencing.sh \
  scripts/benchmark-t203.sh \
  scripts/test-t203-package.sh \
  scripts/race-t203.sh \
  scripts/vet-t203.sh \
  scripts/stage-t203.sh \
  scripts/commit-t203.sh \
  scripts/push-t203.sh

git diff --cached --check
