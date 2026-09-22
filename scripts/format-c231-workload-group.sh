#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/sql_workload_admission.go \
    hat/hatSql/c231_workload_group_test.go \
    hat/hatSql/c231_workload_group_benchmark_test.go
