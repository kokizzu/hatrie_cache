#!/bin/sh
set -eu

gofmt -w \
  hat/hatCache/sql_json_path_skip.go \
  hat/hatCache/sql_json_path_skip_test.go \
  hat/hatCache/sql_json_path_skip_benchmark_test.go \
  hat/hatCache/main.go \
  hat/hatCache/sql_query.go
