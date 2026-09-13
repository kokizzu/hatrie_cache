#!/usr/bin/env bash
set -eu

git status --short
git diff --check
git diff --stat
git diff -- hat/hatCache/main.go hat/hatCache/leveldb_store.go hat/hatCache/local_partition.go README.md BENCHMARK.md INSPIRATION_BACKLOG.md Makefile
rg -n "func Open.*Pebble|type PebbleStore|SpillCold" hat/hatCache/pebble_store.go hat/hatCache/storage_backend_test.go
sed -n '220,285p' hat/hatCache/storage_backend_test.go
