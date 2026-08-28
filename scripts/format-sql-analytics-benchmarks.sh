#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/graph.go hat/hatSql/analytics_benchmark_test.go
