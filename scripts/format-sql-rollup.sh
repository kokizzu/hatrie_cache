#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/rollup.go hat/hatSql/rollup_test.go
