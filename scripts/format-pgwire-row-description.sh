#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPgWire/server.go hat/hatPgWire/backend_row_description_reuse_test.go hat/hatPgWire/backend_row_description_reuse_benchmark_test.go
