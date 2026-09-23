#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/ch005_runtime_join_partition_filter.go hat/hatSql/ch005_runtime_join_partition_filter_benchmark_test.go
