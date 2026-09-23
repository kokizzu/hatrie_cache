#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/scheduled_snapshot.go \
  hat/hatCache/snapshot_manifest.go \
  hat/hatCache/t213_scheduled_snapshot_baseline_benchmark_test.go \
  hat/hatCache/t213_scheduled_snapshot_test.go
