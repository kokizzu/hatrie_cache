#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatRuntime/sandbox.go \
  hat/hatRuntime/tu04_sandbox_test.go \
  hat/hatRuntime/tu04_sandbox_benchmark_test.go
