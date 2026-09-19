#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatMemoryStats/report.go hat/hatMemoryStats/report_test.go hat/hatMemoryStats/report_bounds_test.go hat/hatMemoryStats/report_benchmark_test.go
