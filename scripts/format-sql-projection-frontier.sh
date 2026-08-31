#!/bin/sh
set -eu

gofmt -w hat/hatCache/sql_projection_frontier.go hat/hatCache/sql_projection_frontier_test.go hat/hatCache/sql_projection_frontier_benchmark_test.go
