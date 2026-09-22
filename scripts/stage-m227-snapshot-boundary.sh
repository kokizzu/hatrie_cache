#!/usr/bin/env bash
set -euo pipefail

if [[ -n "$(git diff --cached --name-only)" ]]; then
  printf '%s\n' 'refusing to stage M227: index already contains changes' >&2
  git diff --cached --name-only >&2
  exit 1
fi

paths=(
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

git add -- "${paths[@]}"
git diff --cached --check
git diff --cached --name-only
