#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatFiber -run '^$' -bench '^BenchmarkTU30' -benchmem -count=5 -benchtime=50ms
