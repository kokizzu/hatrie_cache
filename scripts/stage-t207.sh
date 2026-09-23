#!/usr/bin/env bash
set -euo pipefail

allowed=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  T207_REPLICA_RECOVERY.md
  hat/hatReplication/tu207_replica_recovery.go
  hat/hatReplication/tu207_replica_recovery_test.go
  hat/hatReplication/tu207_replica_recovery_benchmark_test.go
  scripts/benchmark-t207-before.sh
  scripts/benchmark-t207.sh
  scripts/commit-t207.sh
  scripts/format-t207.sh
  scripts/push-t207.sh
  scripts/race-t207.sh
  scripts/stage-t207.sh
  scripts/test-t207-package.sh
  scripts/test-t207.sh
  scripts/vet-t207.sh
)

git add -- "${allowed[@]}"

is_allowed() {
  local candidate="$1"
  local path
  for path in "${allowed[@]}"; do
    if [[ "${candidate}" == "${path}" ]]; then
      return 0
    fi
  done
  return 1
}

while IFS= read -r staged_path; do
  if ! is_allowed "${staged_path}"; then
    printf 'Refusing to stage T207 with unrelated staged path: %s\n' "${staged_path}" >&2
    exit 1
  fi
done < <(git diff --cached --name-only)

git diff --cached --check
git diff --cached --name-only
