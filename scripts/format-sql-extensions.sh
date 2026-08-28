#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/extensions.go ./hat/hatSql/extensions_test.go
