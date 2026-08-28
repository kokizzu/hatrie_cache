#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/index_hint.go ./hat/hatSql/index_hint_test.go ./hat/hatSql/query.go
