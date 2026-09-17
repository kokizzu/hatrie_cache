#!/bin/sh
set -eu

gofmt -w \
  hat/hatSql/arrangement_cost.go \
  hat/hatSql/mz032_arrangement_cost_benchmark_test.go \
  hat/hatSql/mz032_arrangement_cost_test.go
