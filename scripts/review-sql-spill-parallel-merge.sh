#!/usr/bin/env sh
set -eu

sh ./scripts/format-sql-spill-parallel-merge.sh
sh ./scripts/test-sql-spill-parallel-merge.sh
sh ./scripts/test-race-sql-spill-parallel-merge.sh
sh ./scripts/benchmark-sql-spill-parallel-merge.sh
go test ./...
git diff --check
git status --short
