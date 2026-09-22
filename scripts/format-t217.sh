#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/columnar_space.go \
  hat/hatDataStructure/t217_columnar_space_test.go \
  hat/hatDataStructure/t217_columnar_space_benchmark_test.go
