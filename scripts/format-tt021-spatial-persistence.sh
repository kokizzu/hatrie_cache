#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatSchema/tt021_spatial_index_persistence.go \
  hat/hatSchema/tt021_spatial_index_persistence_test.go \
  hat/hatSchema/tt021_materialized_spatial_index_benchmark_test.go
