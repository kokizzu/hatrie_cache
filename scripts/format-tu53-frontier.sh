#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/logical_frontier.go \
  hat/hatDataStructure/tu53_frontier_test.go \
  hat/hatDataStructure/tu53_frontier_benchmark_test.go
