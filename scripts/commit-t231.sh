#!/usr/bin/env bash
set -euo pipefail

expected=(
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  Makefile
  README.md
  T231_AFTER_REPLACE_AUDIT.md
  hat/hatDataStructure/space.go
  hat/hatDataStructure/t231_space_after_replace_baseline_test.go
  hat/hatDataStructure/t231_space_after_replace_test.go
  scripts/benchmark-t231-before.sh
  scripts/benchmark-t231.sh
  scripts/commit-t231.sh
  scripts/format-t231.sh
  scripts/push-t231.sh
  scripts/race-t231.sh
  scripts/review-t231.sh
  scripts/stage-t231.sh
  scripts/test-t231-package.sh
  scripts/test-t231.sh
  scripts/verify-t231.sh
  scripts/vet-t231.sh
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

git commit -m "feat: add after-replace audit hooks"
