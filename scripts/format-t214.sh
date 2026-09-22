#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/journal_pull.go hat/hatCache/monitoring.go hat/hatCache/t214_snapshot_stream_test.go hat/hatCache/t214_snapshot_stream_benchmark_test.go hat/hatCache/t214_snapshot_stream_baseline_benchmark_test.go hat/hatCache/t214_snapshot_stream_benchmark_helpers_test.go
