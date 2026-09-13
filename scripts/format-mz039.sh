#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/query.go \
	hat/hatSql/mz039_operator_yield_benchmark_test.go \
	hat/hatSql/mz039_operator_yield_test.go
