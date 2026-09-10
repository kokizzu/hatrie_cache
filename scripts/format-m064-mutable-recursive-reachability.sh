#!/usr/bin/env bash
set -eu

gofmt -w hat/hatSql/recursive_reachability.go hat/hatSql/mutable_recursive_reachability.go hat/hatSql/m064_mutable_recursive_reachability_test.go hat/hatSql/recursive_reachability_benchmark_test.go
