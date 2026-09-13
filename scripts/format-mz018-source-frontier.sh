#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/query.go \
	hat/hatSql/sql_source_frontier_requirement.go \
	hat/hatSql/mz018_source_frontier_wait_test.go \
	hat/hatSql/mz018_source_frontier_wait_benchmark_test.go
gofmt -w hat/hatSql/keyset.go
