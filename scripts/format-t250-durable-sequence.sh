#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/durable_sequence.go \
  hat/hatDataStructure/t250_durable_sequence_test.go \
  hat/hatDataStructure/t250_durable_sequence_baseline_benchmark_test.go \
  hat/hatDataStructure/t250_durable_sequence_benchmark_test.go
