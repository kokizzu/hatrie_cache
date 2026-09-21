#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkC239' -benchmem -count="${COUNT:-5}" -benchtime="${BENCHTIME:-200ms}"
