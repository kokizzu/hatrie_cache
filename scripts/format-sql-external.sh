#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/external.go hat/hatSql/external_test.go
