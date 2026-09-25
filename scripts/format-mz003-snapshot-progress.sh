#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatCache/snapshot_restore_progress.go \
  hat/hatCache/snapshot_restore_staged.go \
  hat/hatCache/mz003_snapshot_restore_progress_test.go \
  hat/hatCache/mz003_snapshot_restore_progress_baseline_test.go \
  hat/hatCache/mz003_snapshot_restore_progress_benchmark_test.go
