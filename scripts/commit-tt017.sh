#!/usr/bin/env bash
set -euo pipefail

git add Makefile README.md BENCHMARK.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md PERSISTENT_STORE_BLOOM_FILTER.md persistent_storage_disk_reserve_api.go cmd/hatrie-cache/main.go cmd/hatrie-cache/main_test.go hat/hatCache/leveldb_store.go hat/hatCache/pebble_store.go hat/hatCache/persistent_store_filters.go hat/hatCache/replication_outbox.go hat/hatCache/run_filter_benchmark_test.go scripts/test-tt017.sh scripts/format-tt017.sh scripts/benchmark-tt017.sh scripts/test-race-tt017.sh scripts/vet-tt017.sh scripts/verify-tt017-docs.sh scripts/review-tt017.sh scripts/commit-tt017.sh scripts/push-tt017.sh
git commit -m 'hatCache: add opt-in persistent run Bloom filters'
