#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m213_differential_consolidation.go \
	hat/hatSql/m213_differential_consolidation_test.go \
	hat/hatSql/m213_differential_consolidation_baseline_benchmark_test.go \
	hat/hatSql/m213_differential_consolidation_benchmark_test.go
