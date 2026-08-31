#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/query.go hat/hatSql/columnar_topn_test.go hat/hatSql/columnar_topn_benchmark_test.go hat/hatSql/columnar_topn_multi_order_benchmark_test.go
