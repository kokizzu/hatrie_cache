#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSql/catalog.go hat/hatSql/mu015_source_status_catalog.go hat/hatSql/mu015_source_status_catalog_baseline_benchmark_test.go hat/hatSql/mu015_source_status_catalog_test.go hat/hatSql/mu015_source_status_catalog_benchmark_test.go
