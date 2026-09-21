#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/tt018_page_index_residency.go \
	hat/hatDataStructure/tt018_page_index_residency_test.go \
	hat/hatDataStructure/tt018_page_index_residency_baseline_test.go \
	hat/hatDataStructure/tt018_page_index_residency_benchmark_test.go
