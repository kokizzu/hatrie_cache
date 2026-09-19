#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMetrics -run '^$' -bench '^BenchmarkT044' -benchmem -count=5
