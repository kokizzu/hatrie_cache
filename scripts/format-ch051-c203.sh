#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatDataStructure/low_cardinality.go hat/hatDataStructure/low_cardinality_test.go hat/hatDataStructure/low_cardinality_benchmark_test.go
