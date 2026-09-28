#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go ./hat/hatPgWire/frontend_buffer_reuse_test.go ./hat/hatPgWire/frontend_buffer_reuse_benchmark_test.go -run '^$' -bench '^BenchmarkReadFrontendMessage(Allocating|Into)$' -benchmem -count=5
