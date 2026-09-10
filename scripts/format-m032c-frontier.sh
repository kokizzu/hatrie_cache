#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/sql_source_frontier_barrier.go \
	hat/hatSql/sql_source_frontier_barrier_test.go \
	hat/hatSql/sql_frontier_barrier_benchmark_test.go
