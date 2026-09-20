#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/conditional_index_catalog.go \
	hat/hatDataStructure/tu24_conditional_index_catalog_test.go \
	hat/hatDataStructure/tu24_conditional_index_catalog_benchmark_test.go
