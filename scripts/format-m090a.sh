#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/sql_adapter.go \
  hat/hatStorage/sql_adapter_baseline_benchmark_test.go \
  hat/hatStorage/sql_resolver_adapter_test.go
