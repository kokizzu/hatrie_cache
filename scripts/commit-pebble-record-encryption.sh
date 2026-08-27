#!/bin/sh
set -eu

git add Makefile hat/hatCache/pebble_generation.go hat/hatCache/pebble_store.go hat/hatCache/persistent_encryption_test.go scripts/commit-pebble-record-encryption.sh scripts/format-persistent-encryption.sh scripts/push-pebble-record-encryption.sh
git commit -m 'feat: encrypt persisted Pebble records'
