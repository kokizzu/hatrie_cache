#!/usr/bin/env bash
set -euo pipefail

test_file="hat/hatDataStructure/t248_retrying_deduplicating_priority_visibility_queue_test.go"
hidden_file="${test_file}.baseline-disabled"
trap 'mv "$hidden_file" "$test_file"' EXIT
mv "$test_file" "$hidden_file"
go test ./hat/hatDataStructure \
	-run '^$' \
	-bench '^BenchmarkT248BaseNackCycle$' \
	-benchmem \
	-count=5
