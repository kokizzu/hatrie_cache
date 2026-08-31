#!/bin/sh
set -eu

gofmt -w hat/hatSql/incremental_projection.go hat/hatSql/incremental_projection_test.go hat/hatSql/incremental_projection_benchmark_test.go hat/hatSql/projection_checkpoint_file.go hat/hatCache/sql_incremental_projection.go hat/hatCache/sql_incremental_projection_test.go
