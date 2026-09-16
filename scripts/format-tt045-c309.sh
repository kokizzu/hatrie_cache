#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/tt045_tuple_compression.go \
  hat/hatDataStructure/tt045_tuple_compression_test.go \
  hat/hatDataStructure/tt045_tuple_compression_benchmark_test.go \
  hat/hatDataStructure/tt045_tuple_compression_size_benchmark_test.go
