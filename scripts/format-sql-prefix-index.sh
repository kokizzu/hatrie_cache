#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	 hat/hatCache/sql_prefix_index_benchmark_test.go \
  hat/hatCache/sql_prefix_index_test.go \
  hat/hatCache/sql_query.go \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go
