#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkRemotePartCacheColumnAware$' -benchmem -benchtime=100ms -count=3
