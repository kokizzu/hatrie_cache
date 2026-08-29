#!/bin/sh
set -eu

git diff --check
git diff -- Makefile
sh ./scripts/test-sql-columnar-like.sh
go test -race ./hat/hatCache
sh ./scripts/benchmark-sql-columnar-single-source.sh
go test ./...
