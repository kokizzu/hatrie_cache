#!/usr/bin/env bash
set -euo pipefail

allowed=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  T220_RTREE_INDEX.md
  scripts/format-t220.sh
  scripts/test-t220.sh
  scripts/benchmark-t220.sh
  scripts/race-t220.sh
  scripts/vet-t220.sh
  scripts/verify-t220-scope.sh
  scripts/stage-t220.sh
  scripts/commit-t220.sh
  scripts/push-t220.sh
)

protected=(
  scripts/commit-c237-projection.sh
  scripts/inspect-c237-row-mapping.sh
  scripts/push-c237-projection.sh
  scripts/stage-c237-projection.sh
)

is_allowed() {
  local candidate=$1
  local path
  for path in "${allowed[@]}"; do
    [[ "$candidate" == "$path" ]] && return 0
  done
  for path in "${protected[@]}"; do
    [[ "$candidate" == "$path" ]] && return 0
  done
  return 1
}

status=$(git status --short --untracked-files=all)
failed=0
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  path=${line:3}
  if [[ "$path" == *" -> "* ]]; then
    path=${path##* -> }
  fi
  if ! is_allowed "$path"; then
    printf 'out-of-scope change: %s\n' "$path" >&2
    failed=1
  fi
done <<< "$status"

if (( failed != 0 )); then
  exit 1
fi

printf 'T220 scope verified.\n'
