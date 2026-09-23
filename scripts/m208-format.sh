#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSql/m208_differential_multiplicity_test.go \
  hat/hatSql/m208_differential_multiplicity_benchmark_test.go
