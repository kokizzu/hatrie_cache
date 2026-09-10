#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/recursive_reachability.go \
  hat/hatSql/recursive_reachability_test.go \
  hat/hatSql/recursive_reachability_benchmark_test.go
