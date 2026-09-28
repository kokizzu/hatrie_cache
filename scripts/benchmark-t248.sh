#!/usr/bin/env bash
set -euo pipefail

GOMAXPROCS=1 go test -run '^$' -bench '^BenchmarkT248VisibilityQueue' -benchmem -benchtime=2s -count=10 ./hat/hatDataStructure
