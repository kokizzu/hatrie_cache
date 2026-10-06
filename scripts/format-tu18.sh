#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/volatile_engine.go \
  hat/hatCache/volatile_engine_test.go \
  hat/hatCache/volatile_engine_benchmark_test.go \
  hat/hatCache/volatile_engine_after_benchmark_test.go
