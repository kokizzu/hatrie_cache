#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/adaptive.go hat/hatSql/adaptive_concurrency_test.go hat/hatSql/adaptive_concurrency_benchmark_test.go
