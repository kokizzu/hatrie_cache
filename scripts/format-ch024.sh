#!/usr/bin/env bash
set -eu

gofmt -w hat/hatSql/columnar_segment_skip_test.go hat/hatSql/ch024_skip_usefulness_benchmark_test.go hat/hatSql/query.go
