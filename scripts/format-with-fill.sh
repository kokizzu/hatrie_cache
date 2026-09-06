#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/with_fill.go \
	hat/hatSql/with_fill_test.go
