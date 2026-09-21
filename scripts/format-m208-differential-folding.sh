#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
    hat/hatSql/m208_differential_folding.go \
    hat/hatSql/m208_differential_folding_test.go \
    hat/hatSql/m208_differential_folding_baseline_benchmark_test.go
