#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatDataStructure/volatile_cache.go \
	hat/hatDataStructure/tu18_volatile_cache_test.go \
	hat/hatDataStructure/tu18_volatile_cache_test.go \
	hat/hatDataStructure/tu18_volatile_cache_benchmark_test.go
