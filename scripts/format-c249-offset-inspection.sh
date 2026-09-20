#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatReplication/c249_offset_inspection_benchmark_test.go \
  hat/hatReplication/c249_offset_inspection_test.go
