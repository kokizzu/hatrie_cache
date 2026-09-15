#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/in_program.go hat/hatSql/chu17_in_program_test.go hat/hatSql/chu17_in_program_benchmark_test.go
