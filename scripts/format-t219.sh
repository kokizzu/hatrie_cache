#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/packed_hash_index.go \
  hat/hatDataStructure/t219_packed_hash_index_test.go \
  hat/hatDataStructure/t219_packed_hash_index_benchmark_test.go
