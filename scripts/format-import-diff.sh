#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/import_diff.go ./hat/hatSql/import_diff_test.go
