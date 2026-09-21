#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatReplication/tt003_route_cache.go \
	hat/hatReplication/tt003_route_cache_baseline_benchmark_test.go \
	hat/hatReplication/tt003_route_cache_benchmark_test.go \
	hat/hatReplication/tt003_route_cache_test.go
