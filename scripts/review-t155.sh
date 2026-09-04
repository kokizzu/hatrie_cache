#!/usr/bin/env bash
set -euo pipefail

files=(
  BENCHMARK.md
  hat/hatCache/command_allocation_budget_test.go
  scripts/benchmark-t155.sh
  scripts/commit-t155.sh
  scripts/format-t155.sh
  scripts/push-t155.sh
  scripts/review-t155.sh
  scripts/stage-t155.sh
  scripts/test-race-t155.sh
  scripts/test-t155.sh
  scripts/vet-t155.sh
)

echo "T155 worktree diff check"
git diff --check -- "${files[@]}"
echo
echo "T155 worktree summary"
git diff --stat -- "${files[@]}"
echo
echo "T155 status"
git status --short
echo
echo "T155 staged paths"
git diff --cached --name-only
