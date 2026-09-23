#!/usr/bin/env bash
set -euo pipefail

expected=(
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  Makefile
  README.md
  T230_ON_REPLACE_CHANGEFEED.md
  hat/hatDataStructure/space.go
  hat/hatDataStructure/t230_space_on_replace_baseline_test.go
  hat/hatDataStructure/t230_space_on_replace_test.go
  scripts/benchmark-t230-before.sh
  scripts/benchmark-t230.sh
  scripts/commit-t230.sh
  scripts/format-t230.sh
  scripts/push-t230.sh
  scripts/race-t230.sh
  scripts/review-t230.sh
  scripts/stage-t230.sh
  scripts/test-t230-package.sh
  scripts/test-t230.sh
  scripts/verify-t230.sh
  scripts/vet-t230.sh
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

git commit -m "feat: add space on-replace changefeed hooks"
