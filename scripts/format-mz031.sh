#!/bin/sh
set -eu

gofmt -w \
  hat/hatSql/c213_incremental_top_k.go \
  hat/hatSql/mz031_rank_changes_test.go \
  hat/hatSql/mz031_rank_changes_benchmark_test.go \
  hat/hatSql/mz031_rank_changes_incremental_benchmark_test.go
