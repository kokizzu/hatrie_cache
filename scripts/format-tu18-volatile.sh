#!/bin/sh
set -eu

gofmt -w api.go hat/hatCache/main.go hat/hatCache/volatile_engine.go hat/hatCache/volatile_engine_test.go hat/hatCache/volatile_engine_benchmark_test.go hat/hatCache/snapshot.go hat/hatCache/backup_bundle.go hat/hatCache/backup_repository.go hat/hatCache/leveldb_store.go hat/hatCache/pebble_store.go
