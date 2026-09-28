#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatRate/rate_limiter.go hat/hatRate/allow_n_test.go hat/hatRate/allow_n_benchmark_test.go
