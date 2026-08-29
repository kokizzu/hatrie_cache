#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/cache_warming.go hat/hatSql/cache_warming_test.go
