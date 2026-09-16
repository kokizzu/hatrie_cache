#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/external.go \
  hat/hatSql/ch_u21_streaming_import_test.go \
  hat/hatSql/ch_u21_streaming_import_baseline_benchmark_test.go \
  hat/hatSql/ch_u21_streaming_import_benchmark_test.go
