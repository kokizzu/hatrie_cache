#!/usr/bin/env bash
set -euo pipefail

git add Makefile \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  C231_SQL_WORKLOAD_GROUPS.md \
  INSPIRATION_ROUND2.md \
  hat/hatSql/query.go \
  hat/hatSql/ch231_workload_groups_test.go \
  hat/hatSql/ch231_workload_groups_benchmark_test.go \
  scripts/test-c231-workload-groups.sh \
  scripts/format-c231-workload-groups.sh \
  scripts/benchmark-c231-workload-groups.sh \
  scripts/race-c231-workload-groups.sh \
  scripts/vet-c231-workload-groups.sh \
  scripts/stage-c231-workload-groups.sh \
  scripts/commit-c231-workload-groups.sh \
  scripts/push-c231-workload-groups.sh
