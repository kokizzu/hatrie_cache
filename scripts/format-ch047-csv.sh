#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/external_csv_parallel.go hat/hatSql/ch047_csv_parallel_test.go hat/hatSql/ch047_csv_parallel_benchmark_test.go
