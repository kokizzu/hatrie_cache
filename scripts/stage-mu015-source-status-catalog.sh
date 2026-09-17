#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md Makefile MU015_SOURCE_STATUS_CATALOG.md PRODUCT_IDEA_GAPS.md README.md \
  hat/hatSql/catalog.go \
  hat/hatSql/mu015_source_status_catalog.go \
  hat/hatSql/mu015_source_status_catalog_baseline_benchmark_test.go \
  hat/hatSql/mu015_source_status_catalog_benchmark_test.go \
  hat/hatSql/mu015_source_status_catalog_test.go \
  scripts/benchmark-mu015-baseline.sh \
  scripts/benchmark-mu015-source-status-catalog.sh \
  scripts/commit-mu015-source-status-catalog.sh \
  scripts/format-mu015-source-status-catalog.sh \
  scripts/push-mu015-source-status-catalog.sh \
  scripts/race-mu015-source-status-catalog.sh \
  scripts/stage-mu015-source-status-catalog.sh \
  scripts/test-mu015-source-status-catalog.sh \
  scripts/test-mu015-source-status-package.sh \
  scripts/vet-mu015-source-status-catalog.sh
git diff --cached --check
git diff --cached --name-status
