#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatPgWire/server.go hat/hatPgWire/frontend_buffer_reuse_test.go hat/hatPgWire/frontend_buffer_reuse_benchmark_test.go
