#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/query.go hat/hatSql/governance.go hat/hatSql/governance_memory_test.go hat/hatSql/c231_workload_groups_benchmark_test.go hat/hatCache/sql_query.go
