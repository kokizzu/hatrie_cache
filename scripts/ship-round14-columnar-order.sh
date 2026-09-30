#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add -- \
  BENCHMARK.md \
  CH050_COLUMNAR_RADIX_ORDER.md \
  INSPIRATION.md \
  Makefile \
  hat/hatSql/round14_columnar_int64_order_test.go \
  hat/hatSql/typed_table_columnar_order.go \
  scripts/benchmark-round14-order.sh \
  scripts/format-round14-order.sh \
  scripts/race-round14-order.sh \
  scripts/ship-round14-columnar-order.sh \
  scripts/test-round14-order.sh \
  scripts/test-round14-package.sh \
  scripts/vet-round14-order.sh
git diff --cached --check
git commit -m 'perf(sql): radix-sort typed columnar order [skip ci]'
git push -u origin codex/next-inspiration-round14
