#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/memtx_table.go \
  hat/hatDataStructure/tu16_memtx_table_test.go \
  hat/hatDataStructure/tu16_memtx_baseline_benchmark_test.go
