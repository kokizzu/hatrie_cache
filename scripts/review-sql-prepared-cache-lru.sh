#!/usr/bin/env sh
set -eu

sh ./scripts/format-sql-prepared-cache-lru.sh
sh ./scripts/test-sql-prepared-cache.sh
sh ./scripts/benchmark-sql-prepared-cache.sh
go test ./...
git diff --check
git status --short
