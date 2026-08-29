#!/bin/sh
set -eu

git diff --check
git diff -- Makefile
sh ./scripts/test-sql-columnar-regexp.sh
go test -race ./hat/hatCache
sh ./scripts/benchmark-sql-columnar-regexp.sh
go test ./...
