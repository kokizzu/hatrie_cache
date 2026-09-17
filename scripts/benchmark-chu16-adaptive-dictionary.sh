#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCHU16' -benchmem -count=5
