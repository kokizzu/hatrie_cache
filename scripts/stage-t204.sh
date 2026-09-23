#!/usr/bin/env bash
set -euo pipefail

git add -- \
  Makefile \
  README.md \
  BENCHMARK.md \
  INSPIRATION_ROUND2.md \
  T204_SUPERVISED_FAILOVER.md \
  hat/hatTopology/election.go \
  hat/hatTopology/supervised_failover.go \
  hat/hatTopology/t204_supervised_failover_test.go \
  hat/hatTopology/t204_supervised_failover_benchmark_test.go \
  hat/hatCache/election.go \
  scripts/format-t204.sh \
  scripts/test-t204-supervised-failover.sh \
  scripts/benchmark-t204.sh \
  scripts/test-t204-package.sh \
  scripts/race-t204.sh \
  scripts/vet-t204.sh \
  scripts/compile-t204-root.sh \
  scripts/stage-t204.sh \
  scripts/commit-t204.sh \
  scripts/push-t204.sh

git diff --cached --check
