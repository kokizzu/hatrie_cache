#!/bin/sh
set -eu

gofmt -w ./hat/hatCache/leveldb_store.go ./hat/hatCache/persistent_encryption.go ./hat/hatCache/persistent_encryption_test.go
