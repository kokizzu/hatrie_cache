#!/bin/sh
set -eu

gofmt -w \
  ./hat/hatSql/pivot.go \
  ./hat/hatSql/pivot_test.go
