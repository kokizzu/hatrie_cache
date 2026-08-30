#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLJSONByteSource$' -benchmem -count=5
