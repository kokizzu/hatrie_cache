#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/sql_workload_admission.go \
	hat/hatSql/chu39_workload_admission_test.go \
	hat/hatSql/chu39_workload_admission_benchmark_test.go
