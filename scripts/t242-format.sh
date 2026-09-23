#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatCache/monitoring.go hat/hatCache/t242_audit_baseline_benchmark_test.go hat/hatCache/t242_audit_test.go
