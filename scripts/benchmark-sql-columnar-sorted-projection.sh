#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLHatTrieColumnarSortedProjection$' -benchmem -count=5
