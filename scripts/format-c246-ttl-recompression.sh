#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/ttl_recompression.go \
  hat/hatDataStructure/ttl_recompression_test.go \
  hat/hatDataStructure/ttl_recompression_benchmark_test.go
