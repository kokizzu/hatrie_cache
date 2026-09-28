#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPgWire/server.go ./hat/hatPgWire/cancel.go ./hat/hatPgWire/metrics.go ./hat/hatPgWire/backend_message_buffer_reuse_benchmark_test.go ./hat/hatPgWire/backend_row_description_reuse_benchmark_test.go -run '^$' -bench '^BenchmarkWriteRowDescription(Allocating|Into)$' -benchmem -count=5
