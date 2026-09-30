#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  BENCHMARK.md
  INSPIRATION.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  M065_DIFFERENTIAL_ROW_NUMBER_LAG.md
  M065_BENCHMARK_RAW.txt
  hat/hatSql/differential_row_number_lag.go
  hat/hatSql/m_u65_differential_row_number_lag_test.go
  hat/hatSql/m_u65_differential_row_number_lag_single_update_test.go
  hat/hatSql/m_u65_differential_row_number_lag_benchmark_test.go
  scripts/format-m065.sh
  scripts/test-m065.sh
  scripts/benchmark-m065.sh
  scripts/race-m065.sh
  scripts/vet-m065.sh
  scripts/status-m065.sh
  scripts/commit-push-m065.sh
)

git add -- "${paths[@]}"
git diff --cached --check
git commit -m 'feat(hatSql): add differential row number lag [skip ci]'
git push -u origin HEAD
