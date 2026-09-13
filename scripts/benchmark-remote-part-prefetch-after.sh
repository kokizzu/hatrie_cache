#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkRemotePartCachePrefetch$' -benchmem -benchtime=100ms -count=3
