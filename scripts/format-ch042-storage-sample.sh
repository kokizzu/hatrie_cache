#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/contracts.go hat/hatSql/query.go hat/hatSql/ch042_storage_sample_test.go hat/hatSql/ch042_storage_sample_benchmark_test.go
