#!/usr/bin/env bash
set -euo pipefail

files=(
  BENCHMARK.md
  INSPIRATION.md
  README.md
  hat/hatCache/async_command_http.go
  hat/hatCache/monitoring.go
  hat/hatCache/slow_command_capture.go
  hat/hatCache/slow_command_capture_test.go
  hat/hatCache/slow_command_capture_benchmark_test.go
  scripts/benchmark-t154.sh
  scripts/commit-t154.sh
  scripts/format-t154.sh
  scripts/push-t154.sh
  scripts/review-t154.sh
  scripts/stage-t154.sh
  scripts/test-race-t154.sh
  scripts/test-t154.sh
  scripts/vet-t154.sh
)

echo "T154 worktree diff check"
git diff --check -- "${files[@]}"
echo
echo "T154 worktree summary"
git diff --stat -- "${files[@]}"
echo
echo "T154 status"
git status --short
echo
echo "T154 staged paths"
git diff --cached --name-only
