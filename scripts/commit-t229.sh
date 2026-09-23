#!/usr/bin/env bash
set -euo pipefail

expected=(
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  Makefile
  README.md
  T229_BEFORE_REPLACE.md
  hat/hatDataStructure/space.go
  hat/hatDataStructure/t229_space_before_replace_baseline_test.go
  hat/hatDataStructure/t229_space_before_replace_test.go
  scripts/benchmark-t229-before.sh
  scripts/benchmark-t229.sh
  scripts/commit-t229.sh
  scripts/format-t229.sh
  scripts/push-t229.sh
  scripts/race-t229.sh
  scripts/review-t229.sh
  scripts/stage-t229.sh
  scripts/test-t229-package.sh
  scripts/test-t229.sh
  scripts/verify-t229.sh
  scripts/vet-t229.sh
)

git diff --cached --check
mapfile -t actual < <(git diff --cached --name-only)
for path in "${actual[@]}"; do
  allowed=0
  for expected_path in "${expected[@]}"; do
    if [[ "$path" == "$expected_path" ]]; then
      allowed=1
      break
    fi
  done
  if [[ "$allowed" -ne 1 ]]; then
    printf 'unexpected staged path: %s\n' "$path" >&2
    exit 1
  fi
done
if [[ "${#actual[@]}" -ne "${#expected[@]}" ]]; then
  printf 'staged path count = %s, want %s\n' "${#actual[@]}" "${#expected[@]}" >&2
  exit 1
fi

git commit -m "feat: add space before-replace hooks"
