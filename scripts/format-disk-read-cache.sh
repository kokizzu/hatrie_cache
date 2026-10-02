#!/bin/sh
set -eu

gofmt -w \
	hat/hatCache/disk_read_cache.go \
	hat/hatCache/disk_read_cache_test.go \
	hat/hatCache/disk_read_cache_benchmark_test.go \
	hat/hatCache/main.go \
	hat/hatCache/memory_compaction.go
