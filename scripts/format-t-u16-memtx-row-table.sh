#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/memtx_row_table.go hat/hatDataStructure/t_u16_memtx_row_table_test.go hat/hatDataStructure/t_u16_memtx_row_table_benchmark_test.go
