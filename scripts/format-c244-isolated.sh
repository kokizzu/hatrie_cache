#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatMerkle/part_manifest.go \
  hat/hatMerkle/c244_part_manifest_test.go \
  hat/hatMerkle/c244_part_manifest_benchmark_test.go
