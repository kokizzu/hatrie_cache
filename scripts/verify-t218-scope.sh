#!/usr/bin/env bash
set -euo pipefail

allowed=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  T218_MULTI_PART_TREE_INDEX.md
  hat/hatDataStructure/ordered_index.go
  hat/hatDataStructure/multi_part_tree_index.go
  hat/hatDataStructure/t218_multi_part_tree_index_benchmark_test.go
  hat/hatDataStructure/t218_multi_part_tree_index_feature_benchmark_test.go
  hat/hatDataStructure/t218_multi_part_tree_index_test.go
  scripts/format-t218.sh
  scripts/test-t218.sh
  scripts/benchmark-t218-before.sh
  scripts/benchmark-t218.sh
  scripts/test-t218-package.sh
  scripts/race-t218.sh
  scripts/vet-t218.sh
  scripts/verify-t218-scope.sh
  scripts/stage-t218.sh
  scripts/commit-t218.sh
  scripts/push-t218.sh
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

printf 'T218 scope verified.\n'
