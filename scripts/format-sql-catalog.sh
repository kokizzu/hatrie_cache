#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/catalog.go ./hat/hatSql/catalog_test.go ./hat/hatSql/query.go
