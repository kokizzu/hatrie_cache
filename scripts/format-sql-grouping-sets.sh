#!/bin/sh
set -eu

gofmt -w \
  ./hat/hatSql/grouping_sets.go \
  ./hat/hatSql/grouping_sets_test.go \
  ./hat/hatSql/query.go
