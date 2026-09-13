#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatCache/pebble_store.go \
  hat/hatCache/tr015_persistent_store_metrics_benchmark_test.go \
  hat/hatCache/tr015_persistent_store_metrics_test.go \
  hat/hatStorage/capabilities.go
