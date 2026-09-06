#!/bin/sh
set -eu

gofmt -w \
	hat/hatBackup/object_store.go \
	hat/hatBackup/object_store_test.go
