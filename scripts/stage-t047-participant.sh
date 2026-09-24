#!/usr/bin/env bash
set -euo pipefail

files=(
  BENCHMARK.md
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
  Makefile
  T047_CLUSTER_WRITE_COMMIT.md
  T047_PARTICIPANT_STATE.md
  hat/hatReplication/cluster_write_commit_participant.go
  hat/hatReplication/cluster_write_commit_participant_benchmark_test.go
  hat/hatReplication/cluster_write_commit_participant_test.go
  scripts/benchmark-t047-participant.sh
  scripts/commit-t047-participant.sh
  scripts/format-t047-participant.sh
  scripts/push-t047-participant.sh
  scripts/race-t047-participant.sh
  scripts/stage-t047-participant.sh
  scripts/test-t047-package.sh
  scripts/test-t047-participant.sh
  scripts/verify-t047-participant-docs.sh
  scripts/vet-t047-participant.sh
)

git add -- "${files[@]}"
git diff --cached --check
git diff --cached --stat
