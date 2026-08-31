#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLHatTrieColumnar.*SegmentSkip$' -benchmem -benchtime=100ms -count=3
