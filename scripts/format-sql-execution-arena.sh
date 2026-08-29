#!/bin/sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/execution_arena_test.go hat/hatSql/execution_arena_benchmark_test.go hat/hatCache/sql_execution_arena_test.go
