#!/usr/bin/env bash
set -euo pipefail

files=(
  INSPIRATION.md
  Makefile
  scripts/commit-t155-checklist.sh
  scripts/push-t155-checklist.sh
  scripts/review-t155-checklist.sh
  scripts/stage-t155-checklist.sh
)

expected=$(printf '%s\n' "${files[@]}" | sort)
actual=$(git diff --cached --name-only | sort)
if [[ "$actual" != "$expected" ]]; then
  echo "staged paths do not match the checklist allowlist" >&2
  printf '%s\n' "$actual" >&2
  exit 1
fi

git diff --cached --check
git diff --quiet -- INSPIRATION.md scripts/commit-t155-checklist.sh scripts/push-t155-checklist.sh scripts/review-t155-checklist.sh scripts/stage-t155-checklist.sh
git commit -m "Mark command allocation budgets complete"
