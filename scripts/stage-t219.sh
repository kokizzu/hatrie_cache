#!/usr/bin/env bash
set -euo pipefail

make verify-t219-scope

paths=(
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

git add -- "${paths[@]}"
git diff --cached --name-only
