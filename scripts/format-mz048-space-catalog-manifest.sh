#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSchema/mz048_catalog_manifest.go \
  hat/hatSchema/mz048_catalog_manifest_test.go \
  hat/hatSchema/mz048_catalog_manifest_benchmark_test.go
