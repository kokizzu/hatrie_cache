#!/usr/bin/env bash
set -euo pipefail

git add BENCHMARK.md C231_WORKLOAD_GROUPS.md INSPIRATION_ROUND2.md Makefile \
    hat/hatSql/sql_workload_admission.go \
    hat/hatSql/c231_workload_group_test.go \
    hat/hatSql/c231_workload_group_benchmark_test.go \
    hat/hatSql/chu39_workload_admission_benchmark_test.go \
    scripts/benchmark-c231-workload-group.sh \
    scripts/commit-c231-workload-group.sh \
    scripts/format-c231-workload-group.sh \
    scripts/inspect-inspiration-ledger.sh \
    scripts/push-c231-workload-group.sh \
    scripts/race-c231-workload-group.sh \
    scripts/stage-c231-workload-group.sh \
    scripts/test-c231-workload-group-package.sh \
    scripts/test-c231-workload-group.sh \
    scripts/vet-c231-workload-group.sh
git diff --cached --check
