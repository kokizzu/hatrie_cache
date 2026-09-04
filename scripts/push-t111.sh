#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/commit-t111.sh
if git diff --cached --quiet -- Makefile BENCHMARK.md INSPIRATION.md README.md cmd/hatrie-cache/main.go cmd/hatrie-cache/main_test.go hat/hatCache/leveldb_store.go hat/hatCache/pebble_generation.go hat/hatCache/pebble_store.go hat/hatCache/storage_size_limit.go hat/hatCache/storage_size_limit_test.go persistent_storage_size_limit_api.go scripts/test-t111.sh scripts/format-t111.sh scripts/benchmark-t111.sh scripts/test-race-t111.sh scripts/vet-t111.sh scripts/review-t111.sh scripts/stage-t111.sh scripts/commit-t111.sh scripts/push-t111.sh; then
  git push origin HEAD
  exit 0
fi
git push origin HEAD
