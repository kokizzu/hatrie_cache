#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/mz028_temporal_interval_arrangement.go \
	hat/hatSql/mz028_temporal_interval_arrangement_test.go \
	hat/hatSql/mz028_temporal_interval_arrangement_baseline_benchmark_test.go \
	hat/hatSql/mz028_temporal_interval_arrangement_benchmark_test.go
