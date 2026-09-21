#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query.go hat/hatSql/ch231_workload_groups_test.go hat/hatSql/ch231_workload_groups_benchmark_test.go
