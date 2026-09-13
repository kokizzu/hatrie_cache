#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkRemotePartCacheBaseline$' -benchmem -count=5
