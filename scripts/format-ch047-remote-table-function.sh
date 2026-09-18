#!/usr/bin/env bash
set -eu

gofmt -w hat/hatSql/ch047_remote_table_function.go hat/hatSql/ch047_remote_table_function_test.go hat/hatSql/ch047_remote_table_function_benchmark_test.go
