#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  INSPIRATION.md \
  Makefile \
  README.md \
  T047_CLUSTER_WRITE_COMMIT.md \
  hat/hatReplication/tu47_cluster_write_commit.go \
  hat/hatReplication/tu47_cluster_write_commit_test.go \
  hat/hatReplication/tu47_cluster_write_commit_benchmark_test.go \
  scripts/benchmark-t047-cluster-write-commit.sh \
  scripts/commit-t047.sh \
  scripts/format-t047-cluster-write-commit.sh \
  scripts/push-t047.sh \
  scripts/race-t047-cluster-write-commit.sh \
  scripts/review-t047.sh \
  scripts/stage-t047.sh \
  scripts/test-t047-cluster-write-commit.sh \
  scripts/test-t047-replication-package.sh \
  scripts/vet-t047-cluster-write-commit.sh
