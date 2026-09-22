#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/memtx_table.go \
  hat/hatDataStructure/t229_before_replace_test.go \
  hat/hatDataStructure/t229_before_replace_benchmark_test.go
