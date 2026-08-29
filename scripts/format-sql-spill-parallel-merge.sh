#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/spill_parallel_merge_test.go
