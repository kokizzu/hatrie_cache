#!/usr/bin/env bash
set -euo pipefail

paths=(
  BENCHMARK.md
  CHU01_DURABLE_ASYNC_INSERT_DEDUP.md
  Makefile
  PRODUCT_IDEA_GAPS.md
  README.md
  hat/hatCache/command_idempotency.go
  hat/hatCache/chu01_idempotency_stats_test.go
  hat/hatCache/journal.go
  scripts/benchmark-chu01-stats.sh
  scripts/ship-chu01-stats.sh
  scripts/test-chu01-idempotency-stats.sh
)

git status --short
git add "${paths[@]}"
git diff --cached --check
git diff --cached --stat
git commit -m "feat: expose async insert idempotency stats [skip ci]"
git push -u origin HEAD
