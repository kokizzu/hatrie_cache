#!/usr/bin/env bash
set -euo pipefail

make verify-t221-scope

paths=(
  Makefile
  README.md
  BENCHMARK.md
  INSPIRATION_ROUND2.md
  T221_BITMAP_INDEX.md
  scripts/format-t221.sh
  scripts/test-t221.sh
  scripts/benchmark-t221.sh
  scripts/race-t221.sh
  scripts/vet-t221.sh
  scripts/verify-t221-scope.sh
  scripts/stage-t221.sh
  scripts/commit-t221.sh
  scripts/push-t221.sh
)

git add -- "${paths[@]}"
git diff --cached --name-only
