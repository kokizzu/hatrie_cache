#!/usr/bin/env bash
set -euo pipefail

gofmt -w api.go hat/hatCache/system_tables.go hat/hatCache/chu36_system_parts_test.go hat/hatCache/chu36_system_parts_benchmark_test.go
