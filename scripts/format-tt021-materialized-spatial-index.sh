#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSchema/materialized.go hat/hatSchema/spatial_index.go hat/hatSchema/tt021_materialized_spatial_index_test.go hat/hatSchema/tt021_materialized_spatial_index_benchmark_test.go
