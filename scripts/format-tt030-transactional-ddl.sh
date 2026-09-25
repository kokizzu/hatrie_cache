#!/usr/bin/env bash
set -euo pipefail
gofmt -w hat/hatSchema/space_catalog.go hat/hatSchema/tt030_transactional_ddl_test.go hat/hatSchema/tt030_transactional_ddl_benchmark_test.go
