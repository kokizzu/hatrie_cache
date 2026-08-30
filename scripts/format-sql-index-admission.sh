#!/bin/sh
set -eu

gofmt -w hat/hatCache/main.go hat/hatCache/sql_query.go hat/hatCache/sql_index_admission_test.go hat/hatCache/sql_index_admission_benchmark_test.go
