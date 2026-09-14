#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/m030_incremental_join.go \
	hat/hatSql/m030_incremental_join_test.go \
	hat/hatSql/m030_incremental_join_benchmark_test.go
