#!/bin/sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'BenchmarkCH029' -benchmem -count=5
