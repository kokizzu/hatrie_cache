#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch050_plan_reproducibility_hash.go hat/hatSql/ch050_plan_reproducibility_hash_test.go hat/hatSql/ch050_plan_reproducibility_hash_benchmark_test.go
