#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMonitoring -run '^$' -bench 'Benchmark(ReadMemoryReport|RuntimeReadMemStats)$' -benchmem -count=5
