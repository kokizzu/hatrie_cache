#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatStorage/remote_part_cache.go \
	hat/hatStorage/remote_part_cache_c243_test.go \
	hat/hatStorage/remote_part_cache_c243_benchmark_test.go
