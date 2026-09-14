#!/usr/bin/env bash
set -euo pipefail

required_files=(
  MZ029_SPILLABLE_ARRANGEMENT.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  README.md
)
for path in "${required_files[@]}"; do
  test -f "$path"
done
grep -nF '# MZ-029 Spillable Arrangements' MZ029_SPILLABLE_ARRANGEMENT.md
grep -nF 'MZ-029 | Spillable arrangements | Adopted as an opt-in bounded local payload spill tier' ENGINE_IDEAS.md
grep -nF 'Materialize MZ-029: Spillable Arrangements' ADOPTED_QUERY_ENGINE_IDEAS.md
grep -nF 'mz-029-spillable-arrangement' BENCHMARK.md
grep -nF 'MZ029_SPILLABLE_ARRANGEMENT.md' README.md
git diff --check
git diff --stat
