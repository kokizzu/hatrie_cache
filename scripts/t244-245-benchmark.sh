#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^Benchmark(DelayQueue|VisibilityQueue)' -benchmem -count=1
