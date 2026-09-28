#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/visibility_queue.go \
  hat/hatDataStructure/t248_dead_letter_test.go \
  hat/hatDataStructure/t248_dead_letter_legacy_benchmark_test.go \
  hat/hatDataStructure/t248_dead_letter_benchmark_test.go
