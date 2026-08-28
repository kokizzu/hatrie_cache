#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/virtual_source.go ./hat/hatSql/virtual_source_test.go
