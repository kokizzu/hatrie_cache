#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatFiber -run '^$' -bench '^BenchmarkTU32' -benchmem -count=5 -benchtime=50ms
