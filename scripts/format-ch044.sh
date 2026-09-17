#!/bin/sh
set -eu
gofmt -w \
  hat/hatCache/main.go \
  hat/hatCache/sql_json_subcolumn.go \
  hat/hatCache/ch044_json_subcolumn_test.go \
  hat/hatCache/ch044_json_subcolumn_benchmark_test.go \
  hat/hatSql/ch031_automatic_json_subcolumn.go
