#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLHatTrieColumnar(Composite)?SortedProjection$' -benchmem -count=5
