#!/usr/bin/env bash
set -euo pipefail

files=(
  BENCHMARK.md
  Makefile
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

expected=$(printf '%s\n' "${files[@]}" | sort)
actual=$(git diff --cached --name-only | sort)
if [[ "$actual" != "$expected" ]]; then
  echo "staged paths do not match the T155 allowlist" >&2
  printf '%s\n' "$actual" >&2
  exit 1
fi

git diff --cached --check
worktree_files=(
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
git diff --quiet -- "${worktree_files[@]}"
git commit -m "Add command allocation budgets"
