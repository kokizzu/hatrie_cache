#!/usr/bin/env bash
set -euo pipefail

allowed_paths=(
  Makefile
  FRONTIER_READ_HOLDS.md
  hat/hatDataStructure/frontier_read_hold.go
  hat/hatDataStructure/frontier_read_hold_test.go
  hat/hatDataStructure/frontier_read_hold_benchmark_test.go
  scripts/test-frontier-read-hold-c203.sh
  scripts/benchmark-frontier-read-hold-c203.sh
  scripts/race-frontier-read-hold-c203.sh
  scripts/vet-frontier-read-hold-c203.sh
  scripts/format-frontier-read-hold-c203.sh
  scripts/stage-frontier-read-hold-c203.sh
  scripts/commit-frontier-read-hold-c203.sh
  scripts/push-frontier-read-hold-c203.sh
)
required_paths=(
  hat/hatDataStructure/frontier_read_hold.go
  hat/hatDataStructure/frontier_read_hold_test.go
  FRONTIER_READ_HOLDS.md
)

if git diff --cached --quiet; then
  printf 'no staged frontier read hold changes\n' >&2
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
  for expected_path in "${allowed_paths[@]}"; do
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

git commit -m 'Add frontier read holds'
