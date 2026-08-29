#!/usr/bin/env sh
set -eu

sh ./scripts/format-sql-columnar-json-benchmark.sh
sh ./scripts/test-sql-columnar-scan.sh
sh ./scripts/benchmark-sql-columnar-scan.sh
go test ./...
git diff --check
git status --short
