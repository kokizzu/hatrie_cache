#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/functional_index.go \
	hat/hatDataStructure/functional_index_test.go \
	hat/hatDataStructure/functional_index_benchmark_test.go
