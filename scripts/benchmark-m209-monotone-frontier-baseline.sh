#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkM209RawAtomicAdvance$' -benchmem -benchtime=2s -count=5
