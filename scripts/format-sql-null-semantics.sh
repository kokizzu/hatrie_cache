#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/null_semantics_test.go ./hat/hatSql/query.go
