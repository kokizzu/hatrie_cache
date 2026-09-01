#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONIndexLiteralIN$' -benchmem -count=5
