#!/usr/bin/env bash
set -euo pipefail

make verify-t220-scope

paths=(
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

git add -- "${paths[@]}"
git diff --cached --name-only
