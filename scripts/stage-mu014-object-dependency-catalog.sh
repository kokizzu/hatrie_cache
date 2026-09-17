#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md Makefile MU014_DEPENDENCY_CATALOG.md PRODUCT_IDEA_GAPS.md README.md \
  hat/hatSql/catalog.go \
  hat/hatSql/mu014_object_dependency_catalog.go \
  hat/hatSql/mu014_object_dependency_catalog_baseline_benchmark_test.go \
  hat/hatSql/mu014_object_dependency_catalog_benchmark_test.go \
  hat/hatSql/mu014_object_dependency_catalog_test.go \
  scripts/benchmark-mu014-baseline.sh \
  scripts/benchmark-mu014-object-dependency-catalog.sh \
  scripts/commit-mu014-object-dependency-catalog.sh \
  scripts/format-mu014-object-dependency-catalog.sh \
  scripts/push-mu014-object-dependency-catalog.sh \
  scripts/race-mu014-object-dependency-catalog.sh \
  scripts/stage-mu014-object-dependency-catalog.sh \
  scripts/test-mu014-object-dependency-catalog.sh \
  scripts/test-mu014-object-dependency-package.sh \
  scripts/vet-mu014-object-dependency-catalog.sh
git diff --cached --check
git diff --cached --name-status
