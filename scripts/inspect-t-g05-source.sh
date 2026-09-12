#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '===== ordered_index.go ====='
awk '{printf "%6d %s\n", NR, $0}' hat/hatDataStructure/ordered_index.go
printf '%s\n' '===== ordered_index_benchmark_test.go ====='
awk '{printf "%6d %s\n", NR, $0}' hat/hatDataStructure/ordered_index_benchmark_test.go
printf '%s\n' '===== ordered_index_test.go ====='
awk '{printf "%6d %s\n", NR, $0}' hat/hatDataStructure/ordered_index_test.go
