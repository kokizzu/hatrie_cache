#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/query.go \
  hat/hatSql/chu05_external_window_stream_test.go \
  hat/hatSql/chu05_external_window_stream_benchmark_test.go
