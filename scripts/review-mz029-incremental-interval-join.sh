#!/usr/bin/env bash
set -euo pipefail

required_files=(
  MZ029_INCREMENTAL_INTERVAL_JOIN.md
  INSPIRATION_BACKLOG.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  README.md
)
for path in "${required_files[@]}"; do
  test -f "$path"
done
grep -nF '# Materialize-Style Incremental Interval Join' MZ029_INCREMENTAL_INTERVAL_JOIN.md
grep -nF 'MZ-29 | Differential interval-join maintenance' INSPIRATION_BACKLOG.md
grep -nF 'Materialize MZ-29: Incremental Interval-Join Maintenance' ADOPTED_QUERY_ENGINE_IDEAS.md
grep -nF 'mz-029-incremental-interval-join' BENCHMARK.md
grep -nF 'MZ029_INCREMENTAL_INTERVAL_JOIN.md' README.md
git diff --check
git diff --stat
