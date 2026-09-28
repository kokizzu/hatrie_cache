#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/top_k.go \
  hat/hatDataStructure/top_k_test.go \
  hat/hatDataStructure/top_k_benchmark_test.go
