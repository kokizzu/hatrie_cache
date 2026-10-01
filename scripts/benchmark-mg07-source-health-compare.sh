#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMetrics -run '^$' -bench 'BenchmarkSource(Health|Frontier)' -benchmem -count=5
