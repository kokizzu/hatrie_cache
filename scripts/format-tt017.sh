#!/usr/bin/env bash
set -euo pipefail

gofmt -w cmd/hatrie-cache/main.go cmd/hatrie-cache/main_test.go hat/hatCache/leveldb_store.go hat/hatCache/pebble_store.go hat/hatCache/persistent_store_filters.go hat/hatCache/replication_outbox.go hat/hatCache/run_filter_benchmark_test.go persistent_storage_disk_reserve_api.go
