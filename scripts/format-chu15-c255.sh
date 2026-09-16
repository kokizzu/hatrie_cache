#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/decimal_kernels.go hat/hatSql/decimal_types.go hat/hatSql/ch_u15_decimal_kernels_test.go
