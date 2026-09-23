#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m212_retained_state_test.go hat/hatSql/m212_retained_state_benchmark_test.go hat/hatSql/retained_state.go
