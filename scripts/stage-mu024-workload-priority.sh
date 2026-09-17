#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md MU024_WORKLOAD_PRIORITIES.md PRODUCT_IDEA_GAPS.md README.md Makefile \
  hat/hatSql/mu023_cluster_admission.go \
  hat/hatSql/mu024_workload_priority_baseline_benchmark_test.go \
  hat/hatSql/mu024_workload_priority_benchmark_test.go \
  hat/hatSql/mu024_workload_priority_test.go \
  scripts/benchmark-mu024-workload-priority-baseline.sh \
  scripts/benchmark-mu024-workload-priority.sh \
  scripts/commit-mu024-workload-priority.sh \
  scripts/format-mu024-workload-priority.sh \
  scripts/push-mu024-workload-priority.sh \
  scripts/race-mu024-workload-priority.sh \
  scripts/review-mu024-workload-priority.sh \
  scripts/stage-mu024-workload-priority.sh \
  scripts/test-mu024-package.sh \
  scripts/test-mu024-workload-priority.sh \
  scripts/verify-mu024-workload-priority.sh \
  scripts/vet-mu024-workload-priority.sh

git diff --cached --check
git diff --cached --stat
git status --short
