#!/usr/bin/env bash
set -euo pipefail

required_files=(
  MZ030_INCREMENTAL_JOIN.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  README.md
)
for path in "${required_files[@]}"; do
  test -f "$path"
done
grep -nF '# MZ-030 Incremental Differential Join' MZ030_INCREMENTAL_JOIN.md
grep -nF 'MZ-030 | Differential join delta maintenance | Adopted as a reusable exact inner-join maintainer' ENGINE_IDEAS.md
grep -nF 'MZ030_INCREMENTAL_JOIN.md' README.md
grep -nF 'MZ-030: Incremental Differential Join' ADOPTED_QUERY_ENGINE_IDEAS.md
grep -nF 'MZ-030 Incremental Differential Join' BENCHMARK.md

git diff --check
git diff --stat
