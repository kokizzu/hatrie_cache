#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=${GOMAXPROCS:-1} go test ./hat/hatAuth -run '^$' -bench '^BenchmarkMU020' -benchmem -count=5 -benchtime=500ms
