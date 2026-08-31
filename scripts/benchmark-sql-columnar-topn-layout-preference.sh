#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLHatTrieColumnarTopNLayoutPreference$' -benchmem -count=5
