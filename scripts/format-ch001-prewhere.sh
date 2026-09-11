#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch001_prewhere_test.go hat/hatSql/ch001_prewhere_benchmark_test.go
