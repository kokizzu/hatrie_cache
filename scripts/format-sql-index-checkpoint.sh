#!/bin/sh
set -eu

gofmt -w \
  hat/hatCache/main.go \
  hat/hatCache/sql_query.go \
  hat/hatCache/sql_index_progress.go \
  hat/hatCache/sql_index_checkpoint.go \
  hat/hatCache/sql_index_checkpoint_test.go \
  hat/hatCache/sql_index_checkpoint_benchmark_test.go
