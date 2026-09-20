#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/tu22_unique_constraint_test.go \
  hat/hatDataStructure/tu22_unique_constraint_benchmark_test.go \
  hat/hatDataStructure/tu22_unique_constraint_baseline_benchmark_test.go
