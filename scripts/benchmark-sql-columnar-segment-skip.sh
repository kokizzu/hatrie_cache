#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLHatTrieColumnarSegmentSkip$' -benchmem -count=5
