#!/usr/bin/env bash
set -euo pipefail

files=(
  INSPIRATION.md
  hat/hatCache/replication_chaos_test.go
  scripts/commit-t152.sh
  scripts/format-t152.sh
  scripts/push-t152.sh
  scripts/review-t152.sh
  scripts/stage-t152.sh
  scripts/test-race-t152.sh
  scripts/test-t152.sh
)

echo "T152 worktree diff check"
git diff --check -- "${files[@]}"
echo
echo "T152 worktree summary"
git diff --stat -- "${files[@]}"
echo
echo "T152 status"
git status --short
echo
echo "T152 staged paths"
git diff --cached --name-only
