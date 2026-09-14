#!/usr/bin/env bash
set -euo pipefail

required_files=(
  MZ031_SKEW_AWARE_JOIN_EXCHANGE.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  README.md
)
for path in "${required_files[@]}"; do
  test -f "$path"
done
grep -nF '# MZ-031 Skew-Aware Join Exchange' MZ031_SKEW_AWARE_JOIN_EXCHANGE.md
grep -nF 'MZ-031 | Skew-aware join exchange | Adopted as an imported deterministic routing policy' ENGINE_IDEAS.md
grep -nF 'Materialize MZ-031: Skew-Aware Join Exchange' ADOPTED_QUERY_ENGINE_IDEAS.md
grep -nF 'mz-031-skew-aware-join-exchange' BENCHMARK.md
grep -nF 'MZ031_SKEW_AWARE_JOIN_EXCHANGE.md' README.md
git diff --check
git diff --stat
