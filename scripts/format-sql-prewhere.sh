#!/bin/sh
set -eu

gofmt -w hat/hatSql/prewhere.go hat/hatSql/prewhere_test.go hat/hatSql/prewhere_benchmark_test.go
