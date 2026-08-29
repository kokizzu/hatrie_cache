#!/usr/bin/env sh
set -eu

sh ./scripts/format-sql-columnar-stream-materialization.sh
sh ./scripts/test-sql-columnar-scan.sh
sh ./scripts/benchmark-sql-columnar-stream-materialization.sh
go test ./...
git diff --check
git status --short
