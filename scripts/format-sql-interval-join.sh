#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/interval_join.go hat/hatSql/interval_join_test.go
