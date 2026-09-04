#!/bin/sh
set -eu

gofmt -w hat/hatSql/columnar_vector_group_aggregate.go hat/hatSql/columnar_vector_group_aggregate_test.go hat/hatSql/columnar_vector_group_aggregate_benchmark_test.go
