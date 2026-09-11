#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md PERSISTENT_STORAGE_DISK_RESERVE.md persistent_storage_disk_reserve_api.go cmd/hatrie-cache/main.go cmd/hatrie-cache/main_test.go hat/hatCache/storage_disk_reserve.go hat/hatCache/storage_disk_reserve_unix.go hat/hatCache/storage_disk_reserve_other.go hat/hatCache/storage_disk_reserve_test.go hat/hatCache/storage_disk_reserve_benchmark_test.go hat/hatCache/pebble_generation.go hat/hatCache/pebble_store.go hat/hatCache/leveldb_store.go scripts/benchmark-tt016-disk-reserve.sh scripts/test-tt016-disk-reserve.sh scripts/test-tt016-cli-config.sh scripts/format-tt016-disk-reserve.sh scripts/verify-tt016-docs.sh scripts/test-race-tt016-disk-reserve.sh scripts/vet-tt016-disk-reserve.sh scripts/review-tt016-disk-reserve.sh scripts/commit-tt016-disk-reserve.sh scripts/push-tt016-disk-reserve.sh
git diff --cached --check
git commit -m 'hatriecache: add persistent-store disk reserve admission'
