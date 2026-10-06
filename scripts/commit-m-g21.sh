#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  ADOPTED_QUERY_ENGINE_IDEAS.md
  M_G21_HYDRATION_PROGRESS.md
  hat/hatSql/typed_table_arrangement_progress.go
  hat/hatSql/typed_table_arrangement_hydration_progress_test.go
  hat/hatSql/typed_table_arrangement_progress_benchmark_test.go
  scripts/benchmark-m-g21.sh
  scripts/commit-m-g21.sh
  scripts/format-m-g21.sh
  scripts/race-m-g21.sh
  scripts/test-m-g21.sh
  scripts/vet-m-g21.sh
)

for path in "${paths[@]}"; do
  git add -- "$path"
done

git diff --cached --check
git diff --cached --stat
git commit -m 'feat: add arrangement hydration progress [skip ci]'
git push -u origin codex/inspiration-m-g21-hydration-progress-20261006
