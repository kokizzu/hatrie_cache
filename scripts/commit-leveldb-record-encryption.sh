#!/bin/sh
set -eu

git add Makefile hat/hatCache/leveldb_store.go hat/hatCache/persistent_encryption.go hat/hatCache/persistent_encryption_test.go scripts/commit-leveldb-record-encryption.sh scripts/format-persistent-encryption.sh scripts/push-leveldb-record-encryption.sh scripts/test-persistent-encryption.sh
git commit -m 'feat: encrypt persisted LevelDB records'
