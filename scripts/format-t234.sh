#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/space.go \
  hat/hatDataStructure/space_conflict.go \
  hat/hatDataStructure/space_transaction.go \
  hat/hatDataStructure/t234_conflict_baseline_benchmark_test.go \
  hat/hatDataStructure/t234_conflict_benchmark_test.go \
  hat/hatDataStructure/t234_space_conflict_test.go
