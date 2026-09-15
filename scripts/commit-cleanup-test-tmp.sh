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
required_paths=(
  scripts/cleanup-test-tmp.sh
  scripts/test-cleanup-test-tmp.sh
)

if git diff --cached --quiet; then
  printf 'no staged cleanup feature changes\n' >&2
  exit 1
fi
git diff --cached --check

for path in "${required_paths[@]}"; do
  git diff --cached --name-only -- "$path" | grep -Fx "$path" > /dev/null || {
    printf 'required staged path is missing: %s\n' "$path" >&2
    exit 1
  }
done

staged_paths="$(git diff --cached --name-only)"
while IFS= read -r path; do
  [[ -n "$path" ]] || continue
  allowed=0
  for expected_path in "${expected_paths[@]}"; do
    if [[ "$path" == "$expected_path" ]]; then
      allowed=1
      break
    fi
  done
  if (( allowed == 0 )); then
    printf 'refusing unexpected staged path: %s\n' "$path" >&2
    exit 1
  fi
done <<< "$staged_paths"

git commit -m 'Add safe stale test temporary cleanup'
