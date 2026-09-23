#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/retained_state.go hat/hatSql/m210_retained_state_test.go hat/hatSql/m210_retained_state_benchmark_test.go
