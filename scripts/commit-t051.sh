#!/usr/bin/env bash
set -euo pipefail

expected_paths=(
  INSPIRATION.md
  Makefile
  README.md
  hat/hatCache/monitoring.go
  hat/hatCache/replication.go
  hat/hatCache/replication_lag_test.go
  hat/hatReplication/model.go
  scripts/commit-t051.sh
  scripts/format-t051.sh
  scripts/push-t051.sh
  scripts/review-t051.sh
  scripts/stage-t051.sh
  scripts/test-race-t051.sh
  scripts/test-t051.sh
  scripts/vet-t051.sh
)

if git diff --cached --quiet --; then
  echo "no staged T051 changes" >&2
  exit 1
fi
git diff --cached --check
mapfile -t staged_paths < <(git diff --cached --name-only)
if [[ "${#staged_paths[@]}" -ne "${#expected_paths[@]}" ]]; then
  echo "unexpected staged path count" >&2
  printf '%s\n' "${staged_paths[@]}" >&2
  exit 1
fi
for path in "${staged_paths[@]}"; do
  found=false
  for expected in "${expected_paths[@]}"; do
    if [[ "$path" == "$expected" ]]; then
      found=true
      break
    fi
  done
  if [[ "$found" != true ]]; then
    echo "unexpected staged path: $path" >&2
    exit 1
  fi
done

git commit -m "ops: expose per-peer replication lag"
