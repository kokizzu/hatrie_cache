#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/tu47_cluster_write_commit_reconcile.go \
  hat/hatReplication/tu47_cluster_write_commit_reconcile_benchmark_test.go \
  hat/hatReplication/tu47_cluster_write_commit_reconcile_test.go
