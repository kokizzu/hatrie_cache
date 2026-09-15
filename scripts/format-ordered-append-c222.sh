#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/ordered_index.go hat/hatDataStructure/ordered_append_fastpath_benchmark_test.go hat/hatDataStructure/ordered_append_fastpath_test.go
