#!/usr/bin/env bash
set -euo pipefail

paths=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  Makefile
  PRODUCT_IDEA_GAPS.md
  TT051_PARTITIONED_DURABILITY.md
  hat/hatCache/partitioned_command_journal.go
  hat/hatCache/tt051_partition_durability_benchmark_test.go
  hat/hatCache/tt051_partition_durability_test.go
  scripts/benchmark-tt051-baseline.sh
  scripts/benchmark-tt051.sh
  scripts/format-tt051.sh
  scripts/race-tt051.sh
  scripts/review-tt051.sh
  scripts/stage-tt051.sh
  scripts/test-tt051.sh
  scripts/vet-tt051.sh
)

git add -- "${paths[@]}"
git diff --cached --check
printf '%s\n' 'Staged TT051 paths:'
git diff --cached --name-status
