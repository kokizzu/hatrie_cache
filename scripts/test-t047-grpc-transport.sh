#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
export GOCACHE="$tmp_dir/gocache"
export GOTMPDIR="$tmp_dir/gotmp"
mkdir -p "$GOTMPDIR"
case "$mode" in
  test)
    go test ./hat/hatCache -run '^TestTU047ClusterWriteCommitGRPC' -count=1
    ;;
  race)
    go test -race ./hat/hatCache -run '^TestTU047ClusterWriteCommitGRPC' -count=1
    ;;
  package)
    go test ./hat/hatCache ./hat/hatReplication
    ;;
  format)
    gofmt -w hat/hatCache/grpc_cluster_write_commit.go hat/hatCache/tu47_cluster_write_commit_grpc_test.go hat/hatCache/tu47_cluster_write_commit_grpc_benchmark_test.go
    ;;
  benchmark)
    go test ./hat/hatCache -run '^$' -bench '^BenchmarkTU047ClusterWriteCommitGRPC$' -benchmem -count=5
    ;;
  *)
    printf 'usage: %s {test|race|package|format|benchmark}\n' "$0" >&2
    exit 2
    ;;
esac
