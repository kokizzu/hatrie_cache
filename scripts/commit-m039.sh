#!/usr/bin/env bash
set -euo pipefail

git diff --check
git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  Makefile \
  M039_PARTITION_ORDER_DECLARATIONS.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatSql/explain_pipeline.go \
  hat/hatSql/m_u39_partition_order_benchmark_test.go \
  hat/hatSql/m_u39_partition_order_test.go \
  hat/hatSql/model.go \
  hat/hatSql/partition_order.go \
  hat/hatSql/query.go \
  scripts/benchmark-m039.sh \
  scripts/format-m039.sh \
  scripts/race-m039.sh \
  scripts/test-m039-package.sh \
  scripts/test-m039.sh \
  scripts/vet-m039.sh \
  scripts/commit-m039.sh
git commit -m "adopt M-U39 partition order declarations"
