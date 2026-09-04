#!/usr/bin/env bash
set -euo pipefail

files=(
  INSPIRATION.md
  Makefile
  hat/hatCache/replication_chaos_test.go
  scripts/commit-t152.sh
  scripts/format-t152.sh
  scripts/push-t152.sh
  scripts/review-t152.sh
  scripts/stage-t152.sh
  scripts/test-race-t152.sh
  scripts/test-t152.sh
)

expected=$(printf '%s\n' "${files[@]}" | sort)
actual=$(git diff --cached --name-only | sort)
if [[ "$actual" != "$expected" ]]; then
  echo "staged paths do not match the T152 allowlist" >&2
  printf '%s\n' "$actual" >&2
  exit 1
fi

git diff --cached --check
worktree_files=(
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
git diff --quiet -- "${worktree_files[@]}"
git commit -m "Add replication chaos recovery coverage"
