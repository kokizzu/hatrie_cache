#!/usr/bin/env bash
set -euo pipefail

allowed=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  T219_HASH_INDEX.md
  scripts/format-t219.sh
  scripts/test-t219.sh
  scripts/benchmark-t219.sh
  scripts/race-t219.sh
  scripts/vet-t219.sh
  scripts/verify-t219-scope.sh
  scripts/stage-t219.sh
  scripts/commit-t219.sh
  scripts/push-t219.sh
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

printf 'T219 scope verified.\n'
