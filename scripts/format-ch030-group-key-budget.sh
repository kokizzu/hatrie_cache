#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/query.go hat/hatSql/governance.go hat/hatSql/hash_group_aggregate.go hat/hatSql/columnar_vector_group_aggregate.go hat/hatSql/sql_result_cache.go hat/hatSql/max_group_keys_test.go hat/hatSql/max_group_keys_benchmark_test.go hat/hatSql/max_group_keys_guarded_benchmark_test.go
