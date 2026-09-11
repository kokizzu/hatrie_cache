#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSchema/schema.go hat/hatSchema/migration_preview_test.go hat/hatSchema/migration_preview_benchmark_test.go
