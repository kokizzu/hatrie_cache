#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSchema/materialized.go \
	hat/hatSchema/tr024_covering_index_baseline_benchmark_test.go \
	hat/hatSchema/tr024_covering_index_benchmark_test.go \
	hat/hatSchema/tr024_covering_index_test.go
