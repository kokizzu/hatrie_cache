#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/ch233_cpu_budget.go \
	hat/hatSql/ch233_cpu_budget_benchmark_test.go \
	hat/hatSql/ch233_cpu_budget_test.go \
	hat/hatSql/ch233_cpu_clock_linux.go \
	hat/hatSql/ch233_cpu_clock_other.go \
	hat/hatSql/query.go
