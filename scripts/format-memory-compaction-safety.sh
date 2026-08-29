#!/usr/bin/env sh
set -eu

gofmt -w api.go cmd/hatrie-cache/main_test.go hat/hatCache/memory_compaction.go hat/hatCache/memory_compaction_test.go
