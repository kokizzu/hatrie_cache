#!/usr/bin/env bash
set -euo pipefail

git add \
  Makefile \
  README.md \
  BENCHMARK.md \
  INSPIRATION_BACKLOG.md \
  MZ048_CATALOG_MANIFEST_MIGRATIONS.md \
  hat/hatSchema/mz048_catalog_manifest.go \
  hat/hatSchema/mz048_catalog_manifest_test.go \
  hat/hatSchema/mz048_catalog_manifest_benchmark_test.go \
  scripts/format-mz048-space-catalog-manifest.sh \
  scripts/test-mz048-space-catalog-manifest.sh \
  scripts/benchmark-mz048-space-catalog-manifest.sh \
  scripts/test-mz048-package.sh \
  scripts/race-mz048-space-catalog-manifest.sh \
  scripts/vet-mz048-space-catalog-manifest.sh \
  scripts/review-mz048.sh \
  scripts/stage-mz048.sh \
  scripts/commit-mz048.sh \
  scripts/push-mz048.sh
git diff --cached --check
git diff --cached --name-only
