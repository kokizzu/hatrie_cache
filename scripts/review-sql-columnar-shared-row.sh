#!/bin/sh
set -eu

git diff --check
git diff -- Makefile
sh ./scripts/test-sql-columnar-shared-row.sh
go test -race ./hat/hatSql ./hat/hatCache
sh ./scripts/benchmark-sql-columnar-single-source.sh
go test ./...
