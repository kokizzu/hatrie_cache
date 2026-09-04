#!/usr/bin/env bash
set -euo pipefail

files=(
  BENCHMARK.md
  INSPIRATION.md
  README.md
  Makefile
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

expected=$(printf '%s\n' "${files[@]}" | sort)
actual=$(git diff --cached --name-only | sort)
if [[ "$actual" != "$expected" ]]; then
  echo "staged paths do not match the T154 allowlist" >&2
  printf '%s\n' "$actual" >&2
  exit 1
fi

git diff --cached --check
worktree_files=(
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
git diff --quiet -- "${worktree_files[@]}"
git commit -m "Add bounded slow-command capture"
