#!/bin/sh
set -eu

git add -- \
  Makefile \
  README.md \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  PRODUCT_IDEA_GAPS.md \
  T-U18_VOLATILE_CACHE_ENGINE.md \
  api.go \
  hat/hatCache/main.go \
  hat/hatCache/volatile_engine.go \
  hat/hatCache/volatile_engine_test.go \
  hat/hatCache/volatile_engine_benchmark_test.go \
  hat/hatCache/snapshot.go \
  hat/hatCache/backup_bundle.go \
  hat/hatCache/backup_repository.go \
  hat/hatCache/leveldb_store.go \
  hat/hatCache/pebble_store.go \
  scripts/format-tu18-volatile.sh \
  scripts/test-tu18-volatile.sh \
  scripts/test-tu18-hatcache.sh \
  scripts/benchmark-tu18-volatile.sh \
  scripts/race-tu18-volatile.sh \
  scripts/vet-tu18-volatile.sh \
  scripts/review-tu18-volatile.sh \
  scripts/stage-tu18-volatile.sh \
  scripts/commit-tu18-volatile.sh \
  scripts/push-tu18-volatile.sh
