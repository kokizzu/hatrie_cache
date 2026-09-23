#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/c003_parallel_hash_join_shared.go hat/hatSql/ch003_parallel_hash_join_benchmark_test.go hat/hatSql/query.go
