#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/row_binary_stream.go \
  hat/hatSql/row_binary_stream_test.go \
  hat/hatSql/row_binary_stream_benchmark_data_test.go \
  hat/hatSql/row_binary_stream_ndjson_benchmark_test.go \
  hat/hatSql/row_binary_stream_benchmark_test.go \
  hat/hatSql/client.go \
  hat/hatCache/sql_row_binary_http.go \
  hat/hatCache/sql_row_binary_http_test.go \
  hat/hatCache/monitoring.go
