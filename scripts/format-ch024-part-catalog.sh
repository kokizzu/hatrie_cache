#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatMerkle/ch024_part_catalog_test.go \
  hat/hatMerkle/ch024_part_catalog_benchmark_test.go
