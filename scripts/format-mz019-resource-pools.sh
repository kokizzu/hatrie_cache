#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
hat/hatCache/sql_query.go \
hat/hatSql/governance.go \
hat/hatSql/mz019_resource_pool_test.go \
hat/hatSql/mz019_resource_pool_benchmark_test.go
