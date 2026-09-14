#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLHatTrieColumnarStringBloomSegmentIN$' -benchmem -count=5
