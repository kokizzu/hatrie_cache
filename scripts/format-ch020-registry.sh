#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatStorage/ch020_remote_part_registry.go \
  hat/hatStorage/ch020_remote_part_registry_test.go \
  hat/hatStorage/ch020_remote_part_registry_baseline_benchmark_test.go
