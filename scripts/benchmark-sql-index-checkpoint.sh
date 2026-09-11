#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONIndexRebuildCheckpoint$' -benchmem -count=5 -benchtime=100x
