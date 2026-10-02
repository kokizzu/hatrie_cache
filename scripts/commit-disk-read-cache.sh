#!/bin/sh
set -eu

git add \
	BENCHMARK.md \
	DISK_READ_CACHE.md \
	Makefile \
	README.md \
	api.go \
	hat/hatCache/disk_read_cache.go \
	hat/hatCache/disk_read_cache_benchmark_test.go \
	hat/hatCache/disk_read_cache_test.go \
	hat/hatCache/main.go \
	hat/hatCache/memory_compaction.go \
	scripts/benchmark-disk-read-cache.sh \
	scripts/commit-disk-read-cache.sh \
	scripts/format-disk-read-cache.sh \
	scripts/push-disk-read-cache.sh \
	scripts/review-disk-read-cache.sh \
	scripts/test-disk-read-cache.sh \
	scripts/verify-disk-read-cache.sh
git diff --cached --check
git commit -m 'feat: add opt-in disk read cache [skip ci]'
