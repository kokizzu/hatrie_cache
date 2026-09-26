#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/tu47_cluster_write_commit.go \
  hat/hatReplication/tu47_cluster_write_commit_coordinator.go \
  hat/hatReplication/tu47_cluster_write_commit_coordinator_store.go \
  hat/hatReplication/tu47_cluster_write_commit_coordinator_test.go \
  hat/hatReplication/tu47_cluster_write_commit_coordinator_benchmark_test.go
