#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/partial_aggregate_envelope.go \
	hat/hatDataStructure/partial_aggregate_state.go \
	hat/hatDataStructure/partial_aggregate_envelope_test.go \
	hat/hatDataStructure/partial_aggregate_envelope_benchmark_test.go \
	hat/hatCache/partial_aggregate_state.go \
	hat/hatCache/partial_aggregate_envelope_test.go \
	hat/hatCache/partial_aggregate_envelope_benchmark_test.go
