#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatDataStructure/arg_extreme_aggregate_state.go \
  hat/hatDataStructure/c224_arg_extreme_state_test.go \
  hat/hatDataStructure/c224_arg_state_baseline_test.go \
  hat/hatDataStructure/partial_aggregate_envelope.go
