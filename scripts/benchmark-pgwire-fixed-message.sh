#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go ./hat/hatPgWire/backend_message_buffer_reuse_benchmark_test.go ./hat/hatPgWire/backend_fixed_message_reuse_benchmark_test.go -run '^$' -bench '^BenchmarkWrite(AuthenticationOK|ReadyForQuery)(Allocating|Into)$' -benchmem -count=5
