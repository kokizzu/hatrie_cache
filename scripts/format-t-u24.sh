#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSchema/space_catalog.go hat/hatSchema/t_u24_conditional_index_metadata_test.go hat/hatSchema/t_u24_conditional_index_metadata_benchmark_test.go
