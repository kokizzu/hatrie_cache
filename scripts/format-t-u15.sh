#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSchema/space_catalog.go hat/hatSchema/space_catalog_test.go hat/hatSchema/space_catalog_benchmark_test.go
