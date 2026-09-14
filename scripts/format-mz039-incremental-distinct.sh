#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m039_incremental_distinct.go \
	hat/hatSql/m039_incremental_distinct_test.go \
	hat/hatSql/m039_incremental_distinct_benchmark_test.go
