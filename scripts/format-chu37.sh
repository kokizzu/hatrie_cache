#!/usr/bin/env bash
set -euo pipefail

gofmt -w api.go hat/hatCache/system_tables.go hat/hatCache/chu37_system_mutations_test.go hat/hatCache/chu37_system_mutations_benchmark_test.go
