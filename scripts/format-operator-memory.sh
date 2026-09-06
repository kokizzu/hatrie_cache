#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatMetrics/operator_memory.go hat/hatMetrics/operator_memory_test.go
