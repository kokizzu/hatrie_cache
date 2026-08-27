#!/bin/sh
set -eu

gofmt -w ./hat/hatCache/leveldb_store.go ./hat/hatCache/pebble_generation.go ./hat/hatCache/pebble_store.go ./hat/hatCache/persistent_encryption.go ./hat/hatCache/persistent_encryption_test.go
