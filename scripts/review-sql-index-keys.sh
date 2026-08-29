#!/usr/bin/env sh
set -eu

sh ./scripts/format-sql-index-keys.sh
sh ./scripts/test-sql-index-keys.sh
sh ./scripts/benchmark-sql-index-keys.sh
go test ./...
git diff --check
git status --short
