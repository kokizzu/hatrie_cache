#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLColumnarLayoutCache$' -benchmem -count=5
