#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/approx_stream_test.go hat/hatSql/approx_stream_benchmark_test.go hat/hatSql/query.go hat/hatSql/approx_aggregate.go
