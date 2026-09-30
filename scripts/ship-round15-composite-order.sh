#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  BENCHMARK.md \
  CH051_COLUMNAR_COMPOSITE_RADIX_ORDER.md \
  INSPIRATION.md \
  Makefile \
  hat/hatSql/round15_columnar_composite_order_test.go \
  hat/hatSql/typed_table_columnar_order.go \
  scripts/benchmark-round15-composite-order.sh \
  scripts/format-round15-composite-order.sh \
  scripts/race-round15-composite-order.sh \
  scripts/test-round15-composite-order.sh \
  scripts/test-round15-package.sh \
  scripts/verify-round15-composite-order.sh \
  scripts/vet-round15-composite-order.sh \
  scripts/ship-round15-composite-order.sh
git diff --cached --check
git commit -m 'perf(sql): radix-sort composite columnar order [skip ci]'
git push -u origin codex/next-inspiration-round15
