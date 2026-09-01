#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONLowerIndex' -benchmem -count=5
