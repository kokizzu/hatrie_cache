#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatCache/top_k_aggregate.go \
	hat/hatCache/top_k_merge_test.go \
	hat/hatCache/top_k_merge_benchmark_test.go
