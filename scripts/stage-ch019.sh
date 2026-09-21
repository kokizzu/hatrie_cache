#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CH019_REPLICA_PART_CHECKS.md \
  ENGINE_IDEAS.md \
  Makefile \
  README.md \
  hat/hatReplication/ch019_replica_part_repair.go \
  hat/hatReplication/ch019_replica_part_repair_benchmark_test.go \
  hat/hatReplication/ch019_replica_part_repair_test.go \
  scripts/benchmark-ch019-replica-repair.sh \
  scripts/commit-ch019.sh \
  scripts/format-ch019-replica-repair.sh \
  scripts/race-ch019-replica-repair.sh \
  scripts/review-ch019.sh \
  scripts/stage-ch019.sh \
  scripts/push-ch019.sh \
  scripts/test-ch019-replica-repair.sh \
  scripts/test-ch019-replication-package.sh
git diff --cached --check
git status --short
