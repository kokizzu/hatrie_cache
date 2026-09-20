#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkC247' -benchmem -benchtime=200ms -count=5
