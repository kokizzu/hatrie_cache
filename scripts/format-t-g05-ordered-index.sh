#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/ordered_index.go \
  hat/hatDataStructure/ordered_index_benchmark_test.go \
  hat/hatDataStructure/ordered_index_public_test.go \
  hat/hatDataStructure/ordered_index_test.go
