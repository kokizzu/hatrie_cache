#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSchema/materialized.go \
  hat/hatSchema/tr021_online_secondary_index_baseline_benchmark_test.go \
  hat/hatSchema/tr021_online_secondary_index_test.go \
  hat/hatSchema/tr021_online_secondary_index_benchmark_test.go
