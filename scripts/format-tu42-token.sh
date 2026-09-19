#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPagination/token.go hat/hatPagination/token_test.go hat/hatPagination/token_benchmark_test.go
