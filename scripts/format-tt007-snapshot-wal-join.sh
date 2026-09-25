#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  api.go \
  hat/hatCache/tt007_snapshot_wal_join.go \
  hat/hatCache/tt007_snapshot_wal_join_test.go \
  hat/hatCache/tt007_snapshot_wal_join_benchmark_test.go
