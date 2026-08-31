#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLHatTrieColumnarStringBloomSegment' -benchmem -benchtime=100ms -count=3
