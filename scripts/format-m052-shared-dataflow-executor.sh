#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m052_shared_dataflow_executor.go \
	hat/hatSql/m052_shared_dataflow_executor_test.go \
	hat/hatSql/m052_shared_dataflow_executor_benchmark_test.go
