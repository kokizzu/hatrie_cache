#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/storage_disk_reserve.go hat/hatCache/storage_disk_reserve_unix.go hat/hatCache/storage_disk_reserve_other.go hat/hatCache/storage_disk_reserve_test.go hat/hatCache/storage_disk_reserve_benchmark_test.go hat/hatCache/pebble_generation.go hat/hatCache/pebble_store.go hat/hatCache/leveldb_store.go cmd/hatrie-cache/main.go cmd/hatrie-cache/main_test.go persistent_storage_disk_reserve_api.go
