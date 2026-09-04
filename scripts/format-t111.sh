#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  cmd/hatrie-cache/main.go \
  cmd/hatrie-cache/main_test.go \
  hat/hatCache/leveldb_store.go \
  hat/hatCache/pebble_generation.go \
  hat/hatCache/pebble_store.go \
  hat/hatCache/storage_size_limit.go \
  hat/hatCache/storage_size_limit_test.go \
  persistent_storage_size_limit_api.go
