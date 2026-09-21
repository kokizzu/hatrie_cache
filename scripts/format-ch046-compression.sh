#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/columnar_block_stream.go \
  hat/hatSql/ch046_wire_compression_test.go \
  hat/hatSql/ch046_wire_compression_baseline_benchmark_test.go \
  hat/hatSql/ch046_wire_compression_benchmark_test.go
