#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/columnar_block_stream.go \
  hat/hatSql/ch046_wire_dictionary_test.go \
  hat/hatSql/ch046_wire_dictionary_benchmark_test.go
