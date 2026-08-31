#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLHatTrieColumnarDirectedCompositeSortedProjection$' -benchmem -count=5
