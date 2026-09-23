#!/usr/bin/env bash
set -euo pipefail

allowed=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  T208_ANONYMOUS_REPLICAS.md
  hat/hatTopology/quorum_membership.go
  hat/hatTopology/topology.go
  hat/hatTopology/tu13_membership_journal.go
  hat/hatTopology/tu208_anonymous_replica_test.go
  hat/hatReplication/quorum_members.go
  hat/hatReplication/tu10_write_quorum.go
  hat/hatReplication/tu208_anonymous_replica_test.go
  hat/hatReplication/tu208_anonymous_quorum_baseline_benchmark_test.go
  hat/hatReplication/tu208_anonymous_quorum_benchmark_test.go
  scripts/benchmark-t208-before.sh
  scripts/benchmark-t208.sh
  scripts/cleanup-go-build-tmp-safe.sh
  scripts/commit-t208.sh
  scripts/format-t208.sh
  scripts/push-t208.sh
  scripts/race-t208.sh
  scripts/stage-t208.sh
  scripts/test-t208-package.sh
  scripts/test-t208.sh
  scripts/vet-t208.sh
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
    printf 'Refusing to stage T208 with unrelated staged path: %s\n' "${staged_path}" >&2
    exit 1
  fi
done < <(git diff --cached --name-only)

git diff --cached --check
git diff --cached --name-only
