#!/usr/bin/env bash
set -euo pipefail
git status --short
git diff --check
git diff --stat
git diff -- hat/hatSql/query.go hat/hatSql/governance.go hat/hatSql/governance_memory_test.go hat/hatSql/c231_workload_groups_benchmark_test.go hat/hatCache/sql_query.go C231_WORKLOAD_GROUPS.md INSPIRATION_ROUND2.md BENCHMARK.md
