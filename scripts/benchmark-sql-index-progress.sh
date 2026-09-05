#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONIndexRebuildProgress$' -benchmem -count=5
