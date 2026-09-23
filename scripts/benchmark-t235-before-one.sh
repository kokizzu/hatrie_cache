#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatFiber -run '^$' -bench '^BenchmarkT235GoroutineWorkerBaseline$' -benchmem -count=1
