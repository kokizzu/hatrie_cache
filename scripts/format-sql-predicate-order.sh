#!/bin/sh
set -eu

gofmt -w \
	hat/hatSql/numeric_predicate_order.go \
	hat/hatSql/numeric_predicate_order_test.go \
	hat/hatSql/numeric_predicate_order_benchmark_test.go
