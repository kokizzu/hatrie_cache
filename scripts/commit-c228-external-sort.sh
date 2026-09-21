#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C228_EXTERNAL_SORT_STABILITY.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatSql/ch228_external_sort_stability_benchmark_test.go \
  hat/hatSql/ch228_external_sort_stability_test.go \
  scripts/benchmark-c228-external-sort.sh \
  scripts/commit-c228-external-sort.sh \
  scripts/format-c228-external-sort.sh \
  scripts/race-c228-external-sort.sh \
  scripts/push-c228-external-sort.sh \
  scripts/test-c228-external-sort.sh \
  scripts/test-c228-package.sh \
  scripts/vet-c228-external-sort.sh
git commit -m "docs: verify stable external sort runs"
