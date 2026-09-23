#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/snapshot.go \
  hat/hatCache/snapshot_restore_staged.go \
  hat/hatCache/t214_snapshot_stream_baseline_benchmark_test.go \
  hat/hatCache/t214_snapshot_stream_test.go
