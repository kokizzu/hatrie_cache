#!/usr/bin/env bash
set -eu

git add Makefile README.md BENCHMARK.md INSPIRATION_BACKLOG.md TR016_STORAGE_PINNING.md hat/hatCache/main.go hat/hatCache/leveldb_store.go hat/hatCache/local_partition.go hat/hatCache/storage_pinning.go hat/hatCache/tr016_storage_pinning_test.go hat/hatCache/tr016_storage_pinning_benchmark_test.go scripts/format-tr016.sh scripts/test-tr016.sh scripts/test-tr016-race.sh scripts/test-tr016-full.sh scripts/benchmark-tr016.sh scripts/status-tr016.sh scripts/review-tr016.sh scripts/commit-tr016.sh scripts/push-tr016.sh
git commit -m 'feat: add storage key pinning policy'
