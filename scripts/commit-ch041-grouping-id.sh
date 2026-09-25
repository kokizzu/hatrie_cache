#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/stage-ch041-grouping-id.sh

expected=(
  BENCHMARK.md
  CH041_GROUPING_PLAN_SHARING.md
  Makefile
  hat/hatSql/ch041_grouping_id_test.go
  hat/hatSql/ch041_one_pass_grouping.go
  hat/hatSql/grouping_sets.go
  scripts/benchmark-ch041-grouping-id.sh
  scripts/commit-ch041-grouping-id.sh
  scripts/format-ch041-grouping-id.sh
  scripts/push-ch041-grouping-id.sh
  scripts/race-ch041-grouping-id.sh
  scripts/stage-ch041-grouping-id.sh
  scripts/test-ch041-grouping-id-package.sh
  scripts/test-ch041-grouping-id.sh
  scripts/vet-ch041-grouping-id.sh
)

declare -A allowed=()
declare -A seen=()
for path in "${expected[@]}"; do
  allowed["$path"]=1
done

staged="$(git diff --cached --name-only)"
if [[ -z "$staged" ]]; then
  printf 'no staged CH-041 files\n' >&2
  exit 1
fi
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  if [[ -z "${allowed[$path]+present}" ]]; then
    printf 'refusing to commit unrelated staged path: %s\n' "$path" >&2
    exit 1
  fi
  seen["$path"]=1
done <<< "$staged"
for path in "${expected[@]}"; do
  if [[ -z "${seen[$path]+present}" ]]; then
    printf 'missing staged CH-041 path: %s\n' "$path" >&2
    exit 1
  fi
done

git diff --cached --check
git commit -m "feat: add multi-argument grouping ids"
