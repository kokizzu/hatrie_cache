#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/mutation_controller.go \
  hat/hatSql/mutation_controller_test.go \
  hat/hatSql/mutation_controller_benchmark_test.go
