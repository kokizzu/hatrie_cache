#!/usr/bin/env bash
set -euo pipefail
git add \
  BENCHMARK.md \
  C231_WORKLOAD_GROUPS.md \
  INSPIRATION_ROUND2.md \
  Makefile \
  hat/hatCache/sql_query.go \
  hat/hatSql/c231_workload_groups_benchmark_test.go \
  hat/hatSql/governance.go \
  hat/hatSql/governance_memory_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-c231-workload-groups.sh \
  scripts/commit-c231-workload-groups.sh \
  scripts/format-c231-workload-groups.sh \
  scripts/push-c231-workload-groups.sh \
  scripts/race-c231-workload-groups.sh \
  scripts/review-c231-workload-groups.sh \
  scripts/test-c231-cache-api.sh \
  scripts/test-c231-package.sh \
  scripts/test-c231-workload-groups.sh \
  scripts/vet-c231-workload-groups.sh
git diff --cached --check
git commit -m 'feat: add namespace memory workload groups [skip ci]'
