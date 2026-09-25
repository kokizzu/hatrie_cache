#!/usr/bin/env bash
set -euo pipefail
gofmt -w \
  hat/hatSchema/tt024_text_index_catalog_test.go \
  hat/hatSchema/tt024_text_index_catalog_benchmark_test.go
