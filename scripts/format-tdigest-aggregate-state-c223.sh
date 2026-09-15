#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/tdigest_aggregate_state.go \
  hat/hatDataStructure/tdigest_aggregate_state_test.go \
  hat/hatDataStructure/tdigest_aggregate_state_benchmark_test.go
