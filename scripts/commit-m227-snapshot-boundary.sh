#!/usr/bin/env bash
set -euo pipefail

expected=(
  Makefile
  INSPIRATION_ROUND2.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  M227_ATOMIC_SNAPSHOT_FRONTIER.md
  hat/hatReplication/m227_snapshot_boundary.go
  hat/hatReplication/m227_snapshot_boundary_test.go
  hat/hatReplication/m227_snapshot_boundary_benchmark_test.go
  scripts/m227-snapshot-boundary.sh
  scripts/stage-m227-snapshot-boundary.sh
  scripts/commit-m227-snapshot-boundary.sh
  scripts/push-m227-snapshot-boundary.sh
)

for path in $(git diff --cached --name-only); do
  allowed=0
  for expected_path in "${expected[@]}"; do
    if [[ "$path" == "$expected_path" ]]; then
      allowed=1
      break
    fi
  done
  if [[ "$allowed" != "1" ]]; then
    printf 'refusing to commit unexpected staged path: %s\n' "$path" >&2
    exit 1
  fi
done

git diff --cached --check
git commit -m "feat: couple snapshot offsets to live frontier"
