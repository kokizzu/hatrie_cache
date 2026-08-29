#!/bin/sh
set -eu

git diff --check
git diff -- Makefile
sh ./scripts/test-sql-execution-arena.sh
go test -race ./hat/hatSql ./hat/hatCache
sh ./scripts/benchmark-sql-execution-arena.sh
go test ./...
