#!/usr/bin/env sh
set -eu

sh ./scripts/format-sql-result-cache-clone.sh
sh ./scripts/test-sql-result-cache.sh
sh ./scripts/benchmark-sql-result-cache.sh
go test ./...
git diff --check
git status --short
