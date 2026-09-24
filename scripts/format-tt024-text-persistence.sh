#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSchema/tt024_text_index_persistence.go \
  hat/hatSchema/tt024_text_proximity_index_benchmark_test.go \
  hat/hatSchema/tt024_text_proximity_index_test.go
