#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  MU031_RETRACTABLE_AGGREGATES.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/mu031_retractable_aggregate_baseline_benchmark_test.go \
  hat/hatSql/mu031_retractable_aggregate_transaction.go \
  hat/hatSql/mu031_retractable_aggregate_transaction_benchmark_test.go \
  hat/hatSql/mu031_retractable_aggregate_transaction_test.go \
  scripts/benchmark-mu031-m043-baseline.sh \
  scripts/benchmark-mu031-m043.sh \
  scripts/commit-mu031-m043.sh \
  scripts/format-mu031-m043.sh \
  scripts/push-mu031-m043.sh \
  scripts/race-mu031-m043.sh \
  scripts/test-all-mu031-m043.sh \
  scripts/test-mu031-m043.sh \
  scripts/test-mu031-package-m043.sh \
  scripts/vet-mu031-m043.sh

git diff --cached --check
git commit -m "feat(sql): add transactional aggregate rollback"
