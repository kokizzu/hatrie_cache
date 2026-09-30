#!/usr/bin/env bash
set -euo pipefail

go test hat/hatDataStructure/roaring.go hat/hatDataStructure/roaring_values_benchmark_test.go \
    -run '^$' -bench '^BenchmarkRoaringBitmapValues$' -benchmem -count=10
