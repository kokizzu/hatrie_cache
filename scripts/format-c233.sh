#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/query.go \
	hat/hatSql/c233_cpu_time_budget_test.go \
	hat/hatSql/c233_cpu_time_budget_benchmark_test.go
