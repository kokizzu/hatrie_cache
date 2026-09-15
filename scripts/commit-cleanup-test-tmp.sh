#!/usr/bin/env bash
set -euo pipefail

expected_paths=(
  Makefile
  scripts/cleanup-test-tmp.sh
  scripts/test-cleanup-test-tmp.sh
  scripts/stage-cleanup-test-tmp.sh
  scripts/commit-cleanup-test-tmp.sh
  scripts/push-cleanup-test-tmp.sh
)

if git diff --cached --quiet; then
  printf 'no staged cleanup feature changes\n' >&2
  exit 1
fi
git diff --cached --check

for path in "${expected_paths[@]}"; do
  git diff --cached --name-only -- "$path" | grep -Fx "$path" > /dev/null || {
    printf 'expected staged path is missing: %s\n' "$path" >&2
    exit 1
  }
done

staged_count="$(git diff --cached --name-only | wc -l)"
if [[ "$staged_count" != "${#expected_paths[@]}" ]]; then
  printf 'refusing to commit unexpected staged paths\n' >&2
  git diff --cached --name-only >&2
  exit 1
fi

git commit -m 'Add safe stale test temporary cleanup'
